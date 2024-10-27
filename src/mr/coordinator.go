package mr

import "log"
import "time"
import "fmt"
import "errors"
import "sync"
import "net"
import "os"
import "net/rpc"
import "net/http"


type taskStatus int

const (
	idle    taskStatus = iota   // 闲置未分配
	running                     // 运行中
	finished                    // 已完成
	failed                      // 失败，需要时间戳机制来辅助
)


// map任务定义
type MapTask struct {
	TaskId int
	Status taskStatus
	StartTime int64    // 任务开始时间戳
}


// Reduce任务定义
type ReduceTask struct {
	Status taskStatus
	StartTime int64    // 任务开始时间戳
}

type Coordinator struct {
	// Your definitions here.
	NReduce int
    MapTasks map[string]*MapTask          // Map任务，键为文件名，值为任务详情
	MapMutex sync.Mutex                   // Map任务互斥锁  
	ReduceTasks []*ReduceTask             // Reduce任务，键为id，值为任务详情
	ReduceMutex sync.Mutex                // Reduce任务互斥锁
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func is_expired(now int64, timestamp int64) bool {
	expire_time := 1000
	ret := ((now - timestamp) > int64(expire_time))
    return ret
}


// 判断是否所有的map任务都已经完成
func (c *Coordinator) isAllMapTaskCompleted() bool {
	for _, taskInfo := range c.MapTasks {
		if taskInfo.Status != finished {
			return false
		}
	}
	return true
}


// 判断是否所有的reduce任务都已经完成
func (c *Coordinator) isAllReduceTaskCompleted() bool {
	for _, taskInfo := range c.ReduceTasks {
		if taskInfo.Status != finished {
			return false
		}
	}
	return true
}


// 私有方法，不对外开放，分配map任务逻辑, 返回值为map任务所对应的文件名, 如果没有则返回空串
func (c *Coordinator) allocMapTask() string {

	alloc_task_statisfied := func(now int64, map_task *MapTask) bool {
		// 若任务状态是idle或者failed则直接分配
		// running的任务需要判断其是否超时，如果超时就分配给它，并且重置任务开始的时间戳
		return map_task.Status == idle || map_task.Status == failed || (map_task.Status == running && is_expired(now, map_task.StartTime))
	}

	for fileName, taskInfo := range c.MapTasks {
		now := time.Now().Unix()
		if alloc_task_statisfied(now, taskInfo) {
			taskInfo.Status = running
			taskInfo.StartTime = now
			return fileName
		}	
	}
	return ""
}


// 私有方法，不对外开放，分配reduce任务逻辑, 返回值为map任务所对应的文件名, 如果没有则返回-1
func (c *Coordinator) allocReduceTask() int {
	alloc_task_statisfied := func(now int64, reduce_task *ReduceTask) bool {
		// 若任务状态是idle或者failed则直接分配
		// running的任务需要判断其是否超时，如果超时就分配给它，并且重置任务开始的时间戳
		return reduce_task.Status == idle || reduce_task.Status == failed || (reduce_task.Status == running && is_expired(now, reduce_task.StartTime))
	}

	for index, taskInfo := range c.ReduceTasks {
		now := time.Now().Unix()
		if alloc_task_statisfied(now, taskInfo) {
			taskInfo.Status = running
			taskInfo.StartTime = now
			return index
		}
	}
	return -1
}


// 服务端rpc接口，用于提供任务
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MsgType != AskForTask {
		return errors.New("an error occured: incorrect msgtype requested")
	}

	// 分配map任务
	c.MapMutex.Lock()
	file_name := c.allocMapTask()
	c.MapMutex.Unlock()
	if len(file_name) > 0 {
		reply.MsgType = MapTaskAlloc
		reply.TaskName = file_name
		reply.NReduce = c.NReduce
		reply.TaskId = c.MapTasks[file_name].TaskId
		fmt.Printf("map task allocated, [%s]\n", file_name)
		return nil
	} else if !c.isAllMapTaskCompleted() {
		// 如果没有map任务可分配，且map任务没有全部执行完毕，则worker侧需要等待
		reply.MsgType = Wait
		fmt.Println("No map task allocated, wait")
		return nil
	}


	// 分配reduce任务
	c.ReduceMutex.Lock()
	index := c.allocReduceTask()
	c.ReduceMutex.Unlock()
	if index >= 0 {
		reply.MsgType = ReduceTaskAlloc
		reply.TaskId = index
		return nil
	} else if !c.isAllReduceTaskCompleted() {
		reply.MsgType = Wait
		return nil
	}

	// 所有任务全部执行完毕，返回Shutdown
	reply.MsgType = Shutdown
	return nil
}


// 服务端rpc接口，用于给客户端报告任务状态
func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	fmt.Printf("Call NoticeResult, req is %+v\n", req)
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


	return ret
}

func (c *Coordinator) initTask(files []string) {
    for idx, filename := range files {
		c.MapTasks[filename] = &MapTask{
			TaskId: idx,
			Status: idle,
		}
	}
	fmt.Printf("Init task success, %d map tasks\n", len(c.MapTasks))
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = &ReduceTask{
			Status: idle,
		}
	}
	fmt.Printf("Init task success, %d reduce tasks\n", len(c.ReduceTasks))
} 

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce: nReduce,
		MapTasks: make(map[string]*MapTask),
		ReduceTasks: make([]*ReduceTask, nReduce),
	}
	// 初始化任务状态
	c.initTask(files)
	c.server()
	return &c
}
