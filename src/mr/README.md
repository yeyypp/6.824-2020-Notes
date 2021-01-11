# Lab1 MapReduce
[Lab intro](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

The basic idea is to have one master and multiple workers. Have the workers to ask for jobs repeatedly until all the jobs have done.

- [worker.go](https://github.com/yeyypp/6.824-2020-Notes/blob/master/src/mr/worker.go)  
It is a good way to start the lab by modifying the worker to send a request for a new job. When the worker accepts the job, it has to choose the relative function to process according to the job state, "Map", "Reduce" or "No job". I set "No job" to indicate all the jobs are sent to workers.  What the worker needs to do is quite similar to the mrsequential. go. Now the problem is what information should we transfer in the "Reply" parameter. The worker needs to report to the master after the current job is done. So there would be two RPC methods, "AskJob" and "ReportJob". We will implement them in the master.go.

- [master.go](https://github.com/yeyypp/6.824-2020-Notes/blob/master/src/mr/master.go)  
According to the paper, the master should contain two different lists to keep the job. And we have to change the specific job state when the job is done or fail to achieve.
For now, we have all the parameters that a "Task" should have. And don't forget the multiple may ask job or report job at the same time. So we should add concurrency control at specific methods or data structure.
