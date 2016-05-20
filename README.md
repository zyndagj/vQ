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
Most importantly, unlike solutions like Makeflow \cite{yu2010harnessing} or TACC's Launcher utilities which assume the tasks in a workflow can be described in a simple text file \cite{wilson2014launcher, eijkhout2012}, vQ requires no modifications to the original tools and works out-of-the-box with a minimal, user-level installation.

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

## TODO
* Support SGE, TORQUE, and LSF
* Support execution on Xeon Phi accelerators
* Support greedy scheduling with shared execution (`-n`)
