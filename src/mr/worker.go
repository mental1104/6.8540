package mr

import "fmt"
import "time"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


type MsgType int

const (
	AskForTask   MsgType = iota    // Worker -> Coordinator 请求任务
	MapTaskAlloc                   // Coordinator -> Worker 分配Map任务
	ReduceTaskAlloc                // Coordinator -> Worker 分配Reduce任务
	MapSuccess                     // Worker -> Coordinator Map任务执行成功
	MapFailed                      // Worker -> Coordinator Map任务执行失败
	ReduceSuccess                  // Worker -> Coordinator Reduce任务执行成功
	ReduceFailed                   // Worker -> Coordinator Reduce任务执行失败
	Shutdown                       // Coordinator -> Worker 没有任务了，可以退出了
	Wait                           // Coordinator -> Worker 当前暂时没有任务分配，需要等待
)


type MessageSend struct {
	MsgType MsgType    // RPC 消息类型
	TaskId int         // 调用NoticeResult时需要指定具体成功/失败的任务ID
}


type MessageReply struct {
	MsgType MsgType  // RPC 消息类型
	NReduce int      // Map任务中，worker需要向coordinator确认reduce结果的数量
	TaskId  int      // 任务ID选取输入文件
	TaskName string  // Map任务专用，Coordinator告知Worker具体该打开哪个文件
}


func CallForTasks() *MessageReply {
    args := MessageSend{
		MsgType: AskForTask,
	}

	reply := MessageReply{}

	ret := call("Coordinator.AskForTask", &args, &reply)
	if ret {
		return &reply
	} else {
		return nil
	}
}


func CallForReportStatus(successType MsgType, taskID int) bool {
	args := MessageSend{
		MsgType: successType,
		TaskId:  taskID,
	}

	ret := call("Coordinator.NoticeResult", &args, nil)

	return ret
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		time.Sleep(1 * time.Second)
		replyMsg := CallForTasks()
		if replyMsg == nil {
			fmt.Println("reply failed!, continue")
			time.Sleep(1 * time.Second)
			continue
		}
		switch replyMsg.MsgType {
		case MapTaskAlloc:
			fmt.Println("MapTaskAlloc")
		case ReduceTaskAlloc:
			fmt.Println("ReduceTaskAlloc")
		case Shutdown:
			fmt.Println("Task completed, Shutdown")
		case Wait:
			fmt.Println("Task blocked, wait")
		default:
			fmt.Printf("Not yet supported type: %d\n", replyMsg.MsgType)
			time.Sleep(1 * time.Second)
		}


	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
