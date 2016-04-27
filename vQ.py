#!/bin/env python

import multiprocessing
import subprocess as sp
import os, re, sys, socket, logging
from threading import Thread, activeCount
from Queue import Queue
from distutils.spawn import find_executable

###############################
# Configure logging
###############################
logger = logging.getLogger('vQ') #Logger name
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler() #Console logger
# Set logging level
ch.setLevel(logging.INFO)
if os.environ.get("VQ_LOG"): ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(name)s - %(asctime)s] %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
#logger.debug('') < info < warn < error < critical

###############################
# Detect system info
###############################
class SysConfig:
	def __init__(self):
		self.getQueue()
		logger.debug("SERVER detected %s scheduler"%(self.queue))
		self.hostName = os.environ.get("HOSTNAME")
		self.getNodes()
		self.cwd = os.getcwd()
		logger.debug("SERVER cwd: %s"%(self.cwd))
		self.getEnv()
	def getNodes(self):
		# TODO: add methods for sge, lsf, pbs
		if self.queue == 'slurm':
			nCMD = "scontrol show hostname $SLURM_NODELIST"
		else:
			logger.error("vQ does not currently support %s"%(self.queue))
			sys.exit(0)
		self.nodeList = sp.check_output(nCMD, shell=True).rstrip('\n').split('\n')
		logger.debug("SERVER found nodes:\n - %s"%('\n - '.join(self.nodeList)))
	def getQueue(self):
		# TODO: add checks for lsf and pbs
		if find_executable('srun'):
			self.queue = 'slurm'
			self.progs = ('srun','sbatch')
		elif find_executable('qsub'):
			self.queue = 'sge'
			self.progs = ('qsub',)
		else:
			self.queue = 'vQ'
		logger.debug("Detected %s scheduler"%(self.queue))
	def getEnv(self):
		# Grab and compile local env for passing
		# taken from: https://github.com/TACC/launcher/blob/master/pass_env
		regex=re.compile("^(LAUNCHER|TACC|ICC|GCC|LMOD|MV2|IMPI|PATH|LD_LIBRARY_PATH|OMP|KMP|MIC|PYTHON)")
		self.env = ' '.join(["%s=%s"%(k,v) for k,v in os.environ.iteritems() if regex.search(k)])
	#def getCPUs(self):

def sanitizeMsg(msg, prog):
	'''
	Function to sanitize out scheduler messages from the individual tasks.

	ARGUMENTS
	=====================
	msg	Message
	prog	Submission program

	OUTPUT	(tuple)
	=====================
	sanMsg	Sanitized message
	stdout	Output file
	stderr	Error file
	name	Job name
	
	>>> sanitizeMsg('cats -a -b two','srun')
	('cats -a -b two', '', '', 'vQ_task')
	>>> sanitizeMsg('cats','srun')
	('cats', '', '', 'vQ_task')
	>>> sanitizeMsg('-o out.txt cats -a -b two','srun')
	('cats -a -b two', 'out.txt', '', 'vQ_task')
	>>> sanitizeMsg('-a cats -b two','srun')
	Traceback (most recent call last):
	SystemExit: 1
	>>> sanitizeMsg('-e out.txt cats -a -b two','srun')
	('cats -a -b two', '', 'out.txt', 'vQ_task')
	>>> sanitizeMsg('-J cat_tasks -e out.txt cats -a -b two','srun')
	('cats -a -b two', '', 'out.txt', 'cat_tasks')
	'''
	sMsg = msg.split(' ')
	out, err, name = ('','','')
	if prog == 'srun':
		slurmNA = [("-O", "--overcommit"), ("-q", "--quit-on-interrupt"), ("-Q", "--quiet"), ("-k", "--no-kill"), ("-E", "--preserve-env"), ("-H", "--hold"), ("-K", "--kill-on-bad-exit"), ("-l", "--label"), ("-s", "--share"), ("-u", "--unbuffered"), ("-v", "--verbose"), ("-X", "--disable-status"), ("-Z", "--no-allocate"), ("-h", "--help"), ("-V", "--version")]
		slurmA = [("-A", "--account=name"), ("-c", "--cpus-per-task=ncpus"), ("-d", "--dependency=type:jobid"), ("-D", "--chdir=path"), ("-e", "--error=err"), ("-i", "--input=in"), ("-I", "--immediate[=secs]"), ("-J", "--job-name=jobname"), ("-L", "--licenses=names"), ("-m", "--distribution=type"), ("-n", "--ntasks=ntasks"), ("-N", "--nodes=N"), ("-o", "--output=out"), ("-p", "--partition=partition"), ("-r", "--relative=n"), ("-S", "--core-spec=cores"), ("-T", "--threads=threads"), ("-t", "--time=minutes"), ("-W", "--wait=sec"), ("-C", "--constraint=list"), ("-w", "--nodelist=hosts..."), ("-x", "--exclude=hosts..."), ("-B", "--extra-node-info=S[:C[:T]]")]
		argI = 0
		while argI < len(sMsg):
			if sMsg[argI][0] != '-':
				break
			# Double dash
			elif sMsg[argI][1] == '-':
				# No args
				if '=' not in sMsg[argI]:
					if sMsg[argI] in map(lambda x:x[1], slurmNA):
						pass
					else:
						logger.error('Non-srun argument before CMD: %s'%(msg))
						sys.exit(1)
				else:
					arg, val = sMsg[argI].split('=')
					if arg in map(lambda x:x[1].split('=')[0], slurmNA):
						if not val:
							logger.error('%s should have a value for srun'%(sMsg[argI]))
							logger.error(msg)
							sys.exit(1)
						elif arg == '--output':
							out = val
						elif arg == '--error':
							err = val
						elif arg == '--job-name':
							name = val
					else:
						logger.error('Non-srun argument before CMD: %s'%(msg))
						sys.exit(1)
					
			# Single dash
			else:
				# No args
				if sMsg[argI] in map(lambda x: x[0], slurmNA):
					pass
				# Args
				elif sMsg[argI] in map(lambda x: x[0], slurmA):
					if sMsg[argI+1][0] == '-':
						logger.error('%s should have a value for srun: %s'%(sMsg[argI]), msg)
						sys.exit(1)
					elif sMsg[argI] == '-o':
						out = sMsg[argI+1]
					elif sMsg[argI] == '-e':
						err = sMsg[argI+1]
					elif sMsg[argI] == '-J':
						name = sMsg[argI+1]
					argI += 1
				else:
					logger.error('Non-srun argument before CMD: %s'%(msg))
					sys.exit(1)
			argI += 1
	if prog == 'sbatch':
		slurmNA = [("-O", "--overcommit"), ("-q", "--quit-on-interrupt"), ("-Q", "--quiet"), ("-k", "--no-kill"), ("-E", "--preserve-env"), ("-H", "--hold"), ("-K", "--kill-on-bad-exit"), ("-l", "--label"), ("-s", "--share"), ("-u", "--unbuffered"), ("-v", "--verbose"), ("-X", "--disable-status"), ("-Z", "--no-allocate"), ("-h", "--help"), ("-V", "--version")]
		slurmA = [("-A", "--account=name"), ("-c", "--cpus-per-task=ncpus"), ("-d", "--dependency=type:jobid"), ("-D", "--chdir=path"), ("-e", "--error=err"), ("-i", "--input=in"), ("-I", "--immediate[=secs]"), ("-J", "--job-name=jobname"), ("-L", "--licenses=names"), ("-m", "--distribution=type"), ("-n", "--ntasks=ntasks"), ("-N", "--nodes=N"), ("-o", "--output=out"), ("-p", "--partition=partition"), ("-r", "--relative=n"), ("-S", "--core-spec=cores"), ("-T", "--threads=threads"), ("-t", "--time=minutes"), ("-W", "--wait=sec"), ("-C", "--constraint=list"), ("-w", "--nodelist=hosts..."), ("-x", "--exclude=hosts..."), ("-B", "--extra-node-info=S[:C[:T]]"), ("-a", "--array=indexes"), ("-M", "--clusters=names"), ("-F", "--nodefile=filename")]
		argI = 0
		while argI < len(sMsg):
			if sMsg[argI][0] != '-':
				break
			# Double dash
			elif sMsg[argI][1] == '-':
				# No args
				if '=' not in sMsg[argI]:
					if sMsg[argI] in map(lambda x:x[1], slurmNA):
						pass
					else:
						logger.error('Non-srun argument before CMD: %s'%(msg))
						sys.exit(1)
				else:
					arg, val = sMsg[argI].split('=')
					if arg in map(lambda x:x[1].split('=')[0], slurmNA):
						if not val:
							logger.error('%s should have a value for srun'%(sMsg[argI]))
							logger.error(msg)
							sys.exit(1)
						elif arg == '--output':
							out = val
						elif arg == '--error':
							err = val
						elif arg == '--job-name':
							name = val
					else:
						logger.error('Non-srun argument before CMD: %s'%(msg))
						sys.exit(1)
					
			# Single dash
			else:
				# No args
				if sMsg[argI] in map(lambda x: x[0], slurmNA):
					pass
				# Args
				elif sMsg[argI] in map(lambda x: x[0], slurmA):
					if sMsg[argI+1][0] == '-':
						logger.error('%s should have a value for srun: %s'%(sMsg[argI]), msg)
						sys.exit(1)
					elif sMsg[argI] == '-o':
						out = sMsg[argI+1]
					elif sMsg[argI] == '-e':
						err = sMsg[argI+1]
					elif sMsg[argI] == '-J':
						name = sMsg[argI+1]
					argI += 1
				else:
					logger.error('Non-srun argument before CMD: %s'%(msg))
					sys.exit(1)
			argI += 1
	if not name: name = 'vQ_task'
	return (' '.join(sMsg[argI:]), out, err, name)

def clientSendWork(clisock):
	# Send command
	msg = ' '.join(sys.argv[1:])
	## todo: parse out srun arguments
	clisock.recv(1)
	logger.debug("Client Sending: %s"%(msg))
	clisock.sendall(msg)
	if sys.argv[1] in ('srun',):
		# Receive output
		logger.debug("Client waiting for sizes")
		sizes = clisock.recv(20)
		logger.debug("Client srun got: %s"%(sizes))
		intSizes = map(int, sizes.split(','))
		retSize = sum(intSizes)
		msg = clisock.recv(retSize)
		if intSizes[0]:
			out = msg[:intSizes[0]].rstrip('\n')
			logger.debug("Client got out: %s"%(out))
		if intSizes[1]:
			err = msg[intSizes[0]:intSizes[1]]
			logger.debug("Client got err: %s"%(err))
		if intSizes[2]:
			ret = msg[-intSizes[2]:]
			logger.debug("Client got ret: %s"%(ret))
		clisock.close()
		sys.exit(int(ret))
	else:
		clisock.close()
		sys.exit(0)

def main():
	# Test to see if a server is already started
	clisock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
	connectError = clisock.connect_ex(('localhost', 23000))
	if connectError: ## Could not connect to server
		global serverConfig
		serverConfig = SysConfig() # env, nodeList, cwd, hostName
		clisock.close()
		srvsock = startServer()
		threadArray = startThreads(serverConfig.nodeList)
		mp = runCMD(serverConfig.progs)
		processTasks(srvsock, mp)
		stop(srvsock, serverConfig.nodeList)
	else:
		isInteractive = clientSendWork(clisock)
		if isInteractive:
			receiveOutput(clisock)

def startServer(port=23000):
	srvsock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
	srvsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	srvsock.bind( ('localhost', port) )
	srvsock.listen(5)
	logger.info('vQ server started on port %i'%(port))
	return srvsock

def addRedirects(sanCmd, sOut, sErr):
	'''
	Adds redirects to command to reduce communication.

	>>> addRedirects('cats','','')
	'cats'
	>>> addRedirects('cats','out','')
	'( cats ) 1> out'
	>>> addRedirects('cats','','err')
	'( cats ) 2> err'
	>>> addRedirects('cats','out','err')
	'( cats ) 1> out 2> err'
	>>> addRedirects('cats','yes','yes')
	'( cats ) &> yes'
	'''
	if sOut and sOut == sErr:
		outCmd = '( %s ) &> %s'%(sanCmd, sOut)
	elif sOut and sErr:
		outCmd = '( %s ) 1> %s 2> %s'%(sanCmd, sOut, sErr)
	elif sOut:
		outCmd = '( %s ) 1> %s'%(sanCmd, sOut)
	elif sErr:
		outCmd = '( %s ) 2> %s'%(sanCmd, sErr)
	else:
		return sanCmd
	return outCmd
	

# Start worker pool
def worker(host):
	for item, taskCounter in iter(wQ.get, 'STOP'):
		jobNum = "%05i"%(taskCounter)
		csock, (h,a) = item
		# Ask for work
		csock.send('1')
		cmd = csock.recv(5000)
		logger.debug("%s worker got: %s"%(host, cmd))
		# Sanitize the cmd of scheduler arguments
		splitCmd = cmd.split(' ')
		sanCmd, sOut, sErr, jName = sanitizeMsg(' '.join(splitCmd[1:]), splitCmd[0]) 
		# Make a task name with counter
		taskName = "%s_%05i"%(jName,taskCounter)
		# Force redirrection on non-interactive commands
		if splitCmd[0] not in ('srun',):# not interactive
			if not sOut: sOut = "%s.o"%(taskName)
			if not sErr: sErr = "%s.e"%(taskName)
		sanCmd = addRedirects(sanCmd, sOut, sErr)
		logger.debug("Added redirects: %s"%(sanCmd))
		# ssh to external nodes
		logger.debug("%s running: %s"%(host, sanCmd))
		if host != serverConfig.hostName:
			sanCmd = "ssh %s 'cd %s && env %s > /dev/null && %s'"%(host, serverConfig.cwd, serverConfig.env, sanCmd)
		# Run the command
		proc = sp.Popen(sanCmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
		# Get the output / Block
		so, se = proc.communicate()
		if splitCmd[0] in ('srun',): # interactive
			rc = str(proc.returncode)
			sizes = map(len, (so, se, rc))
			sizeString = ','.join(map(str, sizes))
			msg = so+se+rc
			# Send output
			logger.debug("%s worker sending: %s"%(host, sizeString))
			csock.sendall(sizeString)
			logger.debug("%s worker sending: %s"%(host, msg))
			csock.sendall(msg)
		# Close client socket
		csock.close()
		# Finish task in queue
		wQ.task_done()
	wQ.task_done()

def startThreads(nodeList):
	# Needs to be global
	global wQ
	wQ = Queue()

	threadArray = []
	for i in range(len(nodeList)):
		threadArray.append(Thread(target=worker, args=[nodeList[i]]))
		threadArray[i].daemon = True
		threadArray[i].start()
	logger.debug('vQ worker pool started with %i workers\n'%(len(nodeList)))
	return threadArray

def runCMD(progs):
	# Run cmd in ARGS and overload srun
	mainCmd = ' '.join(sys.argv[1:])
	logger.info("Launching: %s"%(mainCmd))
	logger.info("Overloading %s"%(', '.join(progs)))
	overloadStr = ' '.join(['%s () { python vQ.py %s \$@; }; export -f %s;'%(prog, prog, prog) for prog in progs])
	unsetStr = ' '.join(['unset -f %s;'%(prog) for prog in progs])
	mp = sp.Popen('bash -c "%s %s; %s"'%(overloadStr, mainCmd, unsetStr), shell=True)
	return mp

def processTasks(srvsock, mp):
	# Listen for incoming sockets
	srvsock.settimeout(3)
	taskCounter = 1
	while 1:
		try:
			wQ.put((srvsock.accept(),taskCounter))
			taskCounter += 1
			logger.debug("Server accepted a connection\n")
		except socket.timeout:
			logger.debug("Checking status of main process")
			pass
		except KeyboardInterrupt:
			logger.debug("Received keyboard interrupt")
			break
		if mp.poll() is not None:
			logger.info("Main process complete")
			logger.info("Killing server and waiting for jobs to complete")
			break

def stop(srvsock, nodeList):
	# Wait until remaining tasks are done
	srvsock.close()
	wQ.join()

	# Stop workers
	sys.stderr.write("Stopping workers... ")
	for i in range(len(nodeList)):
		wQ.put("STOP")
	wQ.join()
	sys.stderr.write("Stopped.\n")

if __name__ == "__main__":
	main()
