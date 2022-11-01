# Lab1 Map Reduce Report

## Design

```
		Worker 						Coordinator
		----> 	AskForTask 			---->
		<---- 	assign Map task 	<----
		----> 	ReduceMap	     	---->
		<----	Check Can Write		<----
CanWrite----> 	FinishMap			---->
Cannot	----> 	abort

		----> 	AskForTask		 	---->
		<---- 	assign Reduce task 	<----
		----> 	ReduceWrite	     	---->
		<----	Check Can Write		<----
CanWrite---->	FinishReduce		---->
Cannot	---->	abort

And c.CheckFail() checks whether running workers delay or crash
To protect from data racing, use lock (chan int) to protect map
```

6.824 Hints suggests using ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.

I use two phases protocol to Finish Write(In lab, I assume wirte always success， so in fact, both of these are not needed. If Write may fail, replacing the state GetWritePerm with Running works)

## Bugs and Solutions

### Data Race

> commit c4affcd: fix lab1 data race (use -race flag)

Use a lock to protect all functions in Coordinator. Even two channals still have bugs.(e.g. the sequences may change unless the first channal size is one)

### Assign one task to one more worker and In task queue, there are two same task

> coomit 0876918: Fix bugs: 1. assign one task to one more worker. 2. (after fixing bug 1) In task queue, there are two same task.

If the task waiting for assigned has the same state with the timeout task(State Dead), then the task could be the Finished task or only the timeout task

### Woker state transition

> commit e45943e: fix log bug: work state transition

When the task cannot get write permission, the state should transfer to Dead if the task state is not Finish. (The condition)

### ALL Solution

Model Checking , Log, Bug reply.

#### Model Checking

Each task has state, if a worker requests, the state transition should be deterministic.

### Log and Bug Reply

We only need coordinator log infomation to analyze and reply the bug. 

To reply the bug, rerunning the test is wrong. Log tells us the branch where the code excute, add a if branch to make the code excute that branch if you cannot understand the bug by simply looking at the log.

