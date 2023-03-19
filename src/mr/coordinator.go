package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Task struct {
	FileName string
	IdMap    int
	IdReduce int
}

type Coordinator struct {
	// Your definitions here.
	State         int       //0是map阶段 1是reduce阶段 2是结束阶段
	NumMapTask    int       //map任务的数量
	NumReduceTask int       //reduce任务的数量
	MapTask       chan Task //保存map任务的通道
	ReduceTask    chan Task //保存reduce任务的通道
	MapTaskFin    chan bool //保存worker发送的完成map任务的信号
	ReduceTaskFin chan bool //保存worker发送的完成reduce任务的信号
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.MapTaskFin) != c.NumMapTask { //说明map任务还没执行完
		mapTask, ok := <-c.MapTask
		if ok {
			reply.XTask = mapTask
		}
	} else if len(c.ReduceTaskFin) != c.NumReduceTask {
		reduceTask, ok := <-c.ReduceTask
		if ok {
			reply.XTask = reduceTask
		}
	}
	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.State = c.State
	return nil
}

func (c *Coordinator) TaskFin(args *ExampleArgs, reply *ExampleReply) error {
	mu.Lock()
	mu.Unlock()
	if len(c.MapTaskFin) != c.NumMapTask {
		c.MapTaskFin <- true
		if len(c.MapTaskFin) == c.NumMapTask {
			c.State = 1
		}
	} else if len(c.ReduceTaskFin) != c.NumReduceTask {
		c.ReduceTaskFin <- true
		if len(c.ReduceTaskFin) == c.NumReduceTask {
			c.State = 2
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if len(c.ReduceTaskFin) == c.NumReduceTask {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:         0,
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
		MapTaskFin:    make(chan bool, len(files)),
		ReduceTaskFin: make(chan bool, nReduce),
	}

	// Your code here.
	//根据输入文件初始化MapTask
	for id, file := range files {
		c.MapTask <- Task{FileName: file, IdMap: id}
	}
	//初始化ReduceTask
	for i := 0; i < nReduce; i++ {
		c.ReduceTask <- Task{IdReduce: i}
	}

	c.server()
	return &c
}
