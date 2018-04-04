#!/usr/bin/env python

import multiprocessing
import subprocess as sp
import os, re, sys, socket, logging, signal
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
		self.master = socket.gethostname()
		self.nodeList = sp.check_output(nCMD, shell=True).rstrip('\n').split('\n')
		logger.debug("SERVER found nodes:\n - %s"%('\n - '.join(self.nodeList)))
	def getQueue(self):
		# TODO: add checks for lsf and pbs
		if find_executable('srun'):
			self.queue = 'slurm'
			self.progs = {'srun':'i','sbatch':'b','scancel':'b'}
		elif find_executable('qsub'):
			self.queue = 'sge'
			self.progs = {'qrsh':'i','qsub':'b','qlogin':'i'}
		else:
			self.queue = 'vQ'
		logger.debug("Detected %s scheduler"%(self.queue))
	def getEnv(self):
		# Grab and compile local env for passing
		# taken from: https://github.com/TACC/launcher/blob/master/pass_env
		regex=re.compile("^(LAUNCHER|TACC|ICC|GCC|LMOD|MV2|IMPI|PATH|LD_LIBRARY_PATH|OMP|KMP|MIC|PYTHON)")
		self.env = ' '.join(["%s=%s"%(k,v) for k,v in os.environ.iteritems() if regex.search(k)])
	#def getCPUs(self):

###############################
# Main server class
###############################
class VqServer:
	def __init__(self, nodeList):
		self.nodeList = nodeList
		self.wQ = Queue()
		self.ppn = os.environ.get("VQ_PPN")
		if self.ppn:
			self.ppn = int(self.ppn)
		else:
			self.ppn = 1
		self.threadArray = []
		self.mainCmd = ' '.join(sys.argv[1:])
	def startWorkers(self):
		self.processArray = [0]*len(self.nodeList)*self.ppn
		self.workerStates = [0]*len(self.nodeList)*self.ppn
		logger.info("Starting server with %i processes per node"%(self.ppn))
		for j in xrange(self.ppn):
			for i in xrange(len(self.nodeList)):
				tID = len(self.nodeList)*j+i
				self.threadArray.append(Thread(target=worker, args=[tID, self.nodeList[i]]))
				self.threadArray[tID].daemon = True
				self.threadArray[tID].start()
		logger.debug('vQ server started with %i workers'%(len(self.threadArray)))
	def startServer(self, port=23000, timeout=2):
		# Start the batch sever
		self.srvsock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		self.srvsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.srvsock.bind( ('localhost', port) )
		self.srvsock.listen(10)
		logger.info('vQ server started on port %i'%(port))
		# Start the cancel server
		self.cansock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		self.cansock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.cansock.bind( ('localhost', port+1) )
		self.cansock.listen(10)
		logger.info('vQ cancel listener started on port %i'%(port+1))
		# Set the timeout
		self.srvsock.settimeout(timeout)
		self.cansock.settimeout(timeout)
	def runCMD(self, progs):
		logger.info("Overloading %s"%(', '.join(progs)))
		overloadStr = ' '.join(['%s () { vQ.py %s \$@; }; export -f %s;'%(prog, prog, prog) for prog in progs])
		unsetStr = ' '.join(['unset -f %s;'%(prog) for prog in progs])
		logger.info("Launching: %s"%(mainCmd))
		self.mainProcess = sp.Popen('bash -c "%s %s; %s"'%(overloadStr, mainCmd, unsetStr), shell=True)
	def listen(self, taskCounter=1):
		while 1:
			# Listen for cancel command
			try:
				cancelmsg, (h,a) = cansock.accept()
				cancelmsg.send('1')
				if cancelmsg.recv(1) == '1':
					logger.warn("vQ received cancel request")
					self.kill()
			except socket.timeout:
				pass
			except KeyboardInterrupt:
				logger.debug("Received keyboard interrupt")
				self.kill()
			# Listen for queue command
			try:
				item = srvsock.accept()
				logger.debug("Server accepted a connection")
				csock, (h,a) = item+tuple()
				# Interactive
				if int(csock.recv(1)):
					wQ.put((item+tuple(), taskCounter))
					logger.debug("Server held an interactive session %i"%(taskCounter))
				# Batch
				else:
					csock.send('1')
					msg = csock.recv(5000)
					csock.close()
					# If job array, add all tasks
					wQ.put(((msg,), taskCounter))
					logger.debug("Server accepted %i: %s"%(taskCounter, msg))
				taskCounter += 1
			except socket.timeout:
				logger.debug("Checking status of main process")
				if (self.mainProcess.poll() is not None) and not sum(self.workerStates):
					logger.info("Main process complete")
					logger.info("Stopping server and waiting for jobs to complete")
					break
			except KeyboardInterrupt:
				logger.debug("Received keyboard interrupt")
				self.kill()
		self.stop()
	def closeSocks(self):
		'''
		Closes all server sockets.
		'''
		self.srvsock.close()
		self.cansock.close()
	def stop(self):
		'''
		Blocks new tasks from submission, waits for jobs to complete,
		and then stops all workers and exits.
		'''
		self.closeSocks()
		logger.info("Blocking queue for work to finish")
		self.wQ.join()
		logger.info("Work finished")
		for i in xrange(len(self.threadArray)):
			self.wQ.put("STOP")
		self.wQ.join()
		logger.info("Workers stopped and queue destroyed")
	def kill(self):
		'''
		Kills all running tasks and exits.
		'''
		logger.info("Killing all processes and exiting.")
		self.closeSocks()
		# Kill work
		for tID in xrange(len(self.processArray)):
			if self.processArray[tID]:
				os.killpg(os.getpgid(self.processArray[tID]), signal.SIGTERM)
			self.threadArray[tID].join()
		# Kill main process
		os.killpg(os.getpgid(self.mainProcess), signal.SIGTERM)
		logger.info("Done")
		sys.exit(1)
class sanitizer:
	def __init__(self, tabFile):
	noArgSD = set([])
	noArgDD = set([])
	argSD = set([])
	argDD = set([])
	sep = ''
	track = {}
	fileName = "%s.tab"%(tabFile)
	if not os.path.exists(fileName):
		logger.critical("No sanitization config %s"%(fileName))
		sys.exit(1)
	for line in open(tabFile, 'r'):
		if line[0] != '#':
			tmp = line.rstrip('\n').split('\t')
			if len(tmp) == 2:
				noArgSD.add(tmp[0])
				noArgDD.add(tmp[1])
			elif len(tmp) == 3:
				argSD.add(tmp[0])
				argDD.add(tmp[1])
				sep = tmp[2]
			elif len(tmp) == 4:
				argSD.add(tmp[0])
				argDD.add(tmp[1])
				sep = tmp[2]
				track[tmp[0]] = tmp[3]
				track[tmp[1]] = tmp[3]
			else:
				logger.critical("The sanitization config %s should not have more %i columns"%(tabFile, len(tmp))
	return noArgSD, noArgDD, argSD, argDD, sep, track
		

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
	array	Job array string
	
	>>> sanitizeMsg('cats -a -b two','srun')
	('cats -a -b two', '', '', 'vQ_task', '')
	>>> sanitizeMsg('cats','srun')
	('cats', '', '', 'vQ_task', '')
	>>> sanitizeMsg('-o out.txt cats -a -b two','srun')
	('cats -a -b two', 'out.txt', '', 'vQ_task', '')
	>>> sanitizeMsg('-a cats -b two','srun')
	Traceback (most recent call last):
	SystemExit: 1
	>>> sanitizeMsg('-e out.txt cats -a -b two','srun')
	('cats -a -b two', '', 'out.txt', 'vQ_task', '')
	>>> sanitizeMsg('--error=out.txt cats -a -b two','srun')
	('cats -a -b two', '', 'out.txt', 'vQ_task', '')
	>>> sanitizeMsg('-a 1-2 --error=out.txt cats -a -b two','srun')
	Traceback (most recent call last):
	SystemExit: 1
	>>> sanitizeMsg('-a 1-2 --error=out.txt cats -a -b two','sbatch')
	('cats -a -b two', '', 'out.txt', 'vQ_task', '1-2')
	>>> sanitizeMsg('--array=1-2 --error=out.txt cats -a -b two','sbatch')
	('cats -a -b two', '', 'out.txt', 'vQ_task', '1-2')
	>>> sanitizeMsg('-J cat_tasks -e out.txt cats -a -b two','srun')
	('cats -a -b two', '', 'out.txt', 'cat_tasks', '')
	'''
	sMsg = msg.split(' ')
	noArgSD, noArgDD, argSD, argDD, sep, track = parseParams(prog)
	argI = 0
	while argI < len(sMsg):
		param = sMsg[argI]
		if param[0] != '-':
		# Moved into non slurm argument space
			break
		elif param[1] == '-':
		# Double dash parameter
			if '=' not in param:
			# No args
				if param in argDD:
					logger.error('%s has parameter %s without argument'%(prog, sMsg[argI]))
					sys.exit(1)
				elif param not in noArgDD:
					logger.error("Non-%s paramer found"%(prog))
					sys.exit(1)
				else:
					pass
			else:
			# Args
				par, arg = param.split('=')
				if par not in argDD:
					logger.error('Non-%s parameter %s found with argument %s'%(prog, par, arg))
					sys.exit(1)
				else:
					if par in track:
						arrayDict[track[par]] = arg
					else:
						pass
		else:
		# Single dash
			if param in noArgSD:
				pass
			elif param in argSD:
				argI += 1
				arg = sMsg[argI]
				if param in track:
					arrayDict[track[param]] = arg
				else:
					pass
			else:
				logger.error("Non-%s parameter %s found"%(prog, param))
				sys.exit(1)
				
		argI += 1
	if not arrayDict['name']:
		arrayDict['name'] = 'vQ_task'
	return (' '.join(sMsg[argI:]), arrayDict['out'], arrayDict['error'], arrayDict['name'], arrayDict['array'])

def clientSendWork(clisock):
	# Send command
	msg = ' '.join(sys.argv[1:])
	if sys.argv[1] == 'scancel':
		clisock.close()
		clisock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		connectError = clisock.connect_ex(('localhost', 23001))
		if connectError:
			logger.critical("Could not connect to the cancel server")
			sys.exit(1)
		msg = clisock.recv(1)
		if msg == '1':
			clisock.send('1')
			clisock.close()
			logger.info("Sent cancel command")
			sys.exit(0)
		else:
			logger.critical("Got wrong message from cancel server")
			sys.exit(1)
	if sys.argv[1] in ('srun',):
		logger.debug("Client sending interative")
		clisock.send('1') # indicates interactive
		logger.debug("Client waiting for recv: %s"%(msg))
		clisock.recv(1)
		logger.debug("Client Sending: %s"%(msg))
		clisock.sendall(msg)
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
			if out: sys.stdout.write(out+"\n")
		if intSizes[1]:
			err = msg[intSizes[0]:intSizes[1]]
			logger.debug("Client got err: %s"%(err))
			if err: sys.stderr.write(err+"\n")
		if intSizes[2]:
			ret = msg[-intSizes[2]:]
			logger.debug("Client got ret: %s"%(ret))
		clisock.close()
		sys.exit(int(ret))
	else:
		logger.debug("Client sending batch")
		clisock.send('0') # indicates batch
		logger.debug("Client waiting for recv: %s"%(msg))
		clisock.recv(1)
		clisock.sendall(msg)
		clisock.close()
		sys.exit(0)

def main():
	# Test to see if a server is already started
	clisock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
	connectError = clisock.connect_ex(('localhost', 23000))
	if connectError: ## Could not connect to server
		clisock.close()
		config = SysConfig() # env, nodeList, cwd, hostName
		server = VqServer(config.nodeList)
		server.startWorkers()
		server.startServer()
		# Run main process
		server.runCMD(config.progs)
		# Listen for incoming work
		server.listen()
	else:
		isInteractive = clientSendWork(clisock)

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
def worker(pid, host):
	# Update this function to reflect the VqServer class
	global popenArray
	global states
	for item, taskCounter in iter(wQ.get, 'STOP'):
		states[pid] = 1
		jobNum = "%05i"%(taskCounter)
		csock = ""
		if len(item) == 1: #batch
			cmd = item[0]
		else: #interactive
			csock, (h,a) = item
		if csock:
			# Ask for work
			csock.send('1')
			cmd = csock.recv(5000)
		logger.debug("%s worker got: %s"%(host, cmd))
		# Sanitize the cmd of scheduler arguments
		splitCmd = cmd.split(' ')
		sanCmd, sOut, sErr, jName, jArray = sanitizeMsg(' '.join(splitCmd[1:]), splitCmd[0]) 
		# Make a task name with counter
		taskName = "%s_%05i"%(jName,taskCounter)
		# Force redirrection on non-interactive commands
		if not csock:# not interactive
			if not sOut: sOut = "%s.o"%(taskName)
			if not sErr: sErr = "%s.e"%(taskName)
		sanCmd = addRedirects(sanCmd, sOut, sErr)
		logger.debug("Added redirects: %s"%(sanCmd))
		# ssh to external nodes
		logger.debug("%s running: %s"%(host, sanCmd))
		passProgs = ('sbatch','qsub')
		if host != serverConfig.hostName:
			overloadStr = ' '.join(['%s () { ssh %s "vQ.py %s $@"; }; export -f %s;'%(prog, serverConfig.master, prog, prog) for prog in passProgs])
			sanCmd = "ssh -t -t %s 'cd %s && export %s  && %s %s'"%(host, serverConfig.cwd, serverConfig.env, overloadStr, sanCmd)
		else:
			overloadStr = ' '.join(['%s () { vQ.py %s $@; }; export -f %s;'%(prog, prog, prog) for prog in passProgs])
			sanCmd = "%s %s"%(overloadStr, sanCmd)
		# Run the command
		proc = sp.Popen(sanCmd, stdout=sp.PIPE, stderr=sp.PIPE, env=os.environ, shell=True)
		popenArray[pid] = proc.pid
		# Get the output / Block
		so, se = proc.communicate()
		if csock: # interactive
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
		states[pid] = 0
	wQ.task_done()

if __name__ == "__main__":
	main()
