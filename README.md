# vQ
A virtual job scheduler for use in a cluster environment.

Core-counts have been climbing, and the bioinformatics community has started scaling vertically with threads.
This helps at the workstation level, but they provide little benefit on high-performance supercomputers.
Supercomputers, like [Stampede](https://portal.tacc.utexas.edu/user-guides/stampede) at The Texas Advanced Computing Center and [Comet](http://www.sdsc.edu/support/user_guides/comet.html) at The San Diego Supercomputing Center, are not monolithic mainframes, but thousands of computers that work cooperatively over a network.

Sequence assembly is among the most arduous of bioinformatics tasks due to the substantial computational resources and large datasets it requires.
To reduce the time it takes to assemble a genome, developers have begun to develop distributed workflows for clusters by submitting concurrent tasks as separate jobs to a centralized scheduler.
These concurrent jobs enable horizontal scaling, but each job needs to wait in line for resources.
When resources are in short supply, the wait times for each task lead to sub-linear speedups.

To improve the run times for such tasks, we developed the vQ, a virtual queue for multi-node jobs that will dynamically balance the execution of tasks issued with normal SLURM, SGE, TORQUE, and OpenLava (LSF) commands across a pre-allocated set of computing nodes on a shared resource.
We show that this drop-in solution removes queue overhead and improves the experience of running complicated workflows like Trinity, SMRT, Falcon, Canu, Celera, and Cluster Flow.
Most importantly, unlike solutions like [Makeflow](http://ccl.cse.nd.edu/software/makeflow/) or TACC's Launcher utilities ([launcher](https://github.com/TACC/launcher) and [pylauncher](https://github.com/TACC/pylauncher)) which assume the tasks in a workflow can be described in a simple text file, vQ requires no modifications to the original tools and works out-of-the-box with a minimal, user-level installation.

## Usage

For programs that normally issue queue commands like Falcon's fc_run.py

```shell
fc_run.py job.cfg
```

you only need to prepend the `vQ.py` program.

```shell
vQ.py fc_run.py job.cfg
```

vQ then does the following:
1. Detects the environment
2. Starts the main worker pool for execution
3. Overloads the batch and interactive scheduler commands with bash functions
4. Launches the original program with the specified arguments
5. Completes tasks
6. Shuts down when tasks are complete and main process has exited

### Multiple tasks per node

If you have a 16-core nodes and want to run 16 serial tasks on each node, simple set the environment variable `VQ_PPN` to 16.

```shell
export VQ_PPN=16
vQ.py fc_run.py job.cfg
```

### Verbose debugging

If you encounter issues with vQ, you can enable verbose logs with `VQ_LOG`.

```shell
export VQ_LOG=True
vQ.py fc_run.py job.cfg
```

## Examples

### Hello World

The program `helloWorld` creates a script to print out the execution hostname (nodename), runs them in parallel, and prints the output after execution is done.
To reduce the communication overhead, i/o is transferred from the workers to the original calling client AFTER execution is completed. Clients print out all stdout and then stderr.

```shell
$ idev -N 2 -n 2
$ vQ.py helloWorld
[vQ - 2016-05-24 14:57:43,852] vQ server started on port 23000
[vQ - 2016-05-24 14:57:43,859] vQ cancel listener started on port 23001
[vQ - 2016-05-24 14:57:43,859] Launching: helloWorld
[vQ - 2016-05-24 14:57:43,859] Overloading srun, sbatch, scancel
Hello from c443-104.stampede.tacc.utexas.edu
Hello from c443-504.stampede.tacc.utexas.edu
Hello from c443-104.stampede.tacc.utexas.edu
Hello from c443-504.stampede.tacc.utexas.edu
Hello from c443-104.stampede.tacc.utexas.edu
[vQ - 2016-05-24 14:57:54,018] Main process complete
[vQ - 2016-05-24 14:57:54,018] Stopping server and waiting for jobs to complete
[vQ - 2016-05-24 14:57:54,018] Stopping workers...
[vQ - 2016-05-24 14:57:54,018] Stopped.
```

Now, the same program with verbose logging enabled

```shell
$ export VQ_LOG=True
gzynda@Sc442-001[test_programs]$ vQ.py helloWorld
[vQ - 2016-05-24 15:21:54,304] Detected slurm scheduler
[vQ - 2016-05-24 15:21:54,304] SERVER detected slurm scheduler
[vQ - 2016-05-24 15:21:54,316] SERVER found nodes:
 - c442-001
 - c442-003
[vQ - 2016-05-24 15:21:54,316] SERVER cwd: /home1/03076/gzynda/vQ/test_programs
[vQ - 2016-05-24 15:21:54,324] vQ server started on port 23000
[vQ - 2016-05-24 15:21:54,331] vQ cancel listener started on port 23001
[vQ - 2016-05-24 15:21:54,331] vQ worker pool started with 2 workers
[vQ - 2016-05-24 15:21:54,331] Launching: helloWorld
[vQ - 2016-05-24 15:21:54,331] Overloading srun, sbatch, scancel
[vQ - 2016-05-24 15:21:54,503] Server accepted a connection
[vQ - 2016-05-24 15:21:54,504] Client Sending: srun ./hello.sh
[vQ - 2016-05-24 15:21:54,504] Client waiting for sizes
[vQ - 2016-05-24 15:21:54,504] c442-001 worker got: srun ./hello.sh
[vQ - 2016-05-24 15:21:54,504] Added redirects: ./hello.sh
[vQ - 2016-05-24 15:21:54,504] c442-001 running: ./hello.sh
[vQ - 2016-05-24 15:21:54,511] c442-001 worker sending: 45,0,1
[vQ - 2016-05-24 15:21:54,511] Client srun got: 45,0,1
[vQ - 2016-05-24 15:21:54,511] c442-001 worker sending: Hello from c442-001.stampede.tacc.utexas.edu
0
[vQ - 2016-05-24 15:21:54,511] Client got out: Hello from c442-001.stampede.tacc.utexas.edu
Hello from c442-001.stampede.tacc.utexas.edu
[vQ - 2016-05-24 15:21:54,511] Client got ret: 0
[vQ - 2016-05-24 15:21:56,506] Server accepted a connection
[vQ - 2016-05-24 15:21:56,506] Client Sending: srun ./hello.sh
[vQ - 2016-05-24 15:21:56,506] Client waiting for sizes
[vQ - 2016-05-24 15:21:56,506] c442-003 worker got: srun ./hello.sh
[vQ - 2016-05-24 15:21:56,507] Added redirects: ./hello.sh
[vQ - 2016-05-24 15:21:56,507] c442-003 running: ./hello.sh
[vQ - 2016-05-24 15:21:57,509] c442-003 worker sending: 46,120,1
[vQ - 2016-05-24 15:21:57,509] c442-003 worker sending: Hello from c442-003.stampede.tacc.utexas.edu
Warning: Permanently added 'c442-003,129.114.72.119' (RSA) to the list of known hosts.
Connection to c442-003 closed.
0
[vQ - 2016-05-24 15:21:57,509] Client srun got: 46,120,1
[vQ - 2016-05-24 15:21:57,509] Client got out: Hello from c442-003.stampede.tacc.utexas.edu
Hello from c442-003.stampede.tacc.utexas.edu
[vQ - 2016-05-24 15:21:57,509] Client got err: Warning: Permanently added 'c442-003,129.114.72.119' (RSA) to the list of
Warning: Permanently added 'c442-003,129.114.72.119' (RSA) to the list of
[vQ - 2016-05-24 15:21:57,509] Client got ret: 0
[vQ - 2016-05-24 15:21:58,508] Server accepted a connection
[vQ - 2016-05-24 15:21:58,509] Client Sending: srun ./hello.sh
[vQ - 2016-05-24 15:21:58,509] Client waiting for sizes
[vQ - 2016-05-24 15:21:58,509] c442-001 worker got: srun ./hello.sh
[vQ - 2016-05-24 15:21:58,509] Added redirects: ./hello.sh
[vQ - 2016-05-24 15:21:58,509] c442-001 running: ./hello.sh
[vQ - 2016-05-24 15:21:58,516] c442-001 worker sending: 45,0,1
[vQ - 2016-05-24 15:21:58,517] c442-001 worker sending: Hello from c442-001.stampede.tacc.utexas.edu
0
[vQ - 2016-05-24 15:21:58,517] Client srun got: 45,0,1
[vQ - 2016-05-24 15:21:58,517] Client got out: Hello from c442-001.stampede.tacc.utexas.edu
Hello from c442-001.stampede.tacc.utexas.edu
[vQ - 2016-05-24 15:21:58,517] Client got ret: 0
[vQ - 2016-05-24 15:22:00,511] Server accepted a connection
[vQ - 2016-05-24 15:22:00,511] Client Sending: srun ./hello.sh
[vQ - 2016-05-24 15:22:00,511] Client waiting for sizes
[vQ - 2016-05-24 15:22:00,511] c442-003 worker got: srun ./hello.sh
[vQ - 2016-05-24 15:22:00,511] Added redirects: ./hello.sh
[vQ - 2016-05-24 15:22:00,512] c442-003 running: ./hello.sh
[vQ - 2016-05-24 15:22:02,513] Server accepted a connection
[vQ - 2016-05-24 15:22:02,513] Client Sending: srun ./hello.sh
[vQ - 2016-05-24 15:22:02,514] Client waiting for sizes
[vQ - 2016-05-24 15:22:02,514] c442-001 worker got: srun ./hello.sh
[vQ - 2016-05-24 15:22:02,514] Added redirects: ./hello.sh
[vQ - 2016-05-24 15:22:02,514] c442-001 running: ./hello.sh
[vQ - 2016-05-24 15:22:02,521] c442-001 worker sending: 45,0,1
[vQ - 2016-05-24 15:22:02,521] c442-001 worker sending: Hello from c442-001.stampede.tacc.utexas.edu
0
[vQ - 2016-05-24 15:22:02,521] Client srun got: 45,0,1
[vQ - 2016-05-24 15:22:02,521] Client got out: Hello from c442-001.stampede.tacc.utexas.edu
Hello from c442-001.stampede.tacc.utexas.edu
[vQ - 2016-05-24 15:22:02,521] Client got ret: 0
[vQ - 2016-05-24 15:22:02,742] c442-003 worker sending: 46,32,1
[vQ - 2016-05-24 15:22:02,742] c442-003 worker sending: Hello from c442-003.stampede.tacc.utexas.edu
Connection to c442-003 closed.
0
[vQ - 2016-05-24 15:22:02,742] Client srun got: 46,32,1
[vQ - 2016-05-24 15:22:02,742] Client got out: Hello from c442-003.stampede.tacc.utexas.edu
Hello from c442-003.stampede.tacc.utexas.edu
[vQ - 2016-05-24 15:22:02,742] Client got err:
[vQ - 2016-05-24 15:22:02,742] Client got ret: 0
[vQ - 2016-05-24 15:22:04,516] Main process complete
[vQ - 2016-05-24 15:22:04,516] Stopping server and waiting for jobs to complete
[vQ - 2016-05-24 15:22:04,516] Stopping workers...
[vQ - 2016-05-24 15:22:04,516] Stopped.
```

### Exit Statuses

If a workflow depends on the exit status from an srun command to know if something went wrong, vQ properly exits with them when there is a problem.
This functionality can be tested with the program `exitTest`.
`exitTest` first tries to `ls` the file cats, which shouldn't exist.
If it can't be found, the non-zero exit status from ls is propagated back out of the srun command to print out "1 - OK" when this expected status is received.
`exitTest` then tries to `ls $PWD`, and execute hostname. These should both yield "0 - OK".

```shell
$ vQ.py exitTest
[vQ - 2016-05-24 15:34:39,109] vQ server started on port 23000
[vQ - 2016-05-24 15:34:39,115] vQ cancel listener started on port 23001
[vQ - 2016-05-24 15:34:39,116] Launching: exitTest
[vQ - 2016-05-24 15:34:39,116] Overloading srun, sbatch, scancel
cancelTest
exitTest
helloWorld
slurmTest
testProg
0 - OK
ls: cannot access cats: No such file or directory
1 - OK
c442-001.stampede.tacc.utexas.edu
0 - OK
[vQ - 2016-05-24 15:34:49,270] Main process complete
[vQ - 2016-05-24 15:34:49,270] Stopping server and waiting for jobs to complete
[vQ - 2016-05-24 15:34:49,270] Stopping workers...
[vQ - 2016-05-24 15:34:49,271] Stopped.
```

## TODO
* Support SGE, TORQUE, and LSF
* Support execution on Xeon Phi accelerators
* Support greedy scheduling with shared execution (`-n`)
