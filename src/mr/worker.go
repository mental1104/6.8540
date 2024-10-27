package mr

import "fmt"
import "time"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "regexp"
import "path/filepath"
import "io"
import "sort"
import "strconv"
import "io/ioutil"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func DelFileByMapId(targetNumber int, path string) error {
	// 创建正则表达式，X 是可变的指定数字
	pattern := fmt.Sprintf(`^mr-out-%d-\d+$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// 遍历文件，查找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			continue // 跳过目录
		}
		fileName := file.Name()
		if regex.MatchString(fileName) {
			// 匹配到了文件，删除它
			filePath := filepath.Join(path, file.Name())
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}


func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.TaskName)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	kva := mapf(reply.TaskName, string(content))
	sort.Sort(ByKey(kva))

	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for _, kv := range kva {
		redId := ihash(kv.Key) % reply.NReduce
		if encoders[redId] == nil {
			tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-map-tmp-%d", redId))
			if err != nil {
				return err
			}
			defer tempFile.Close()
			tempFiles[redId] = tempFile
			encoders[redId] = json.NewEncoder(tempFile)
		}
		err := encoders[redId].Encode(&kv)
		if err != nil {
			return err
		}
	}

	for i, file := range tempFiles {
		if file != nil {
			fileName := file.Name()
			file.Close()
			newName := fmt.Sprintf("mr-out-%d-%d", reply.TaskId, i)
			if err := os.Rename(fileName, newName); err != nil {
				return err
			}
		}
	}

	return nil
}


func DelFileByReduceId(targetNumber int, path string) error {
	// 创建正则表达式，X 是可变的指定数字
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// 遍历文件，查找匹配的文件
	for _, file := range files {
		if file.IsDir() {
			continue // 跳过目录
		}
		fileName := file.Name()
		if regex.MatchString(fileName) {
			// 匹配到了文件，删除它
			filePath := filepath.Join(path, file.Name())
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}


func ReadSpecificFile(targetNumber int, path string) (fileList []*os.File, err error) {
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// 读取当前目录中的文件
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// 遍历文件，查找匹配的文件
	for _, fileEntry := range files {
		if fileEntry.IsDir() {
			continue // 跳过目录
		}
		fileName := fileEntry.Name()
		if regex.MatchString(fileName) {
			filePath := filepath.Join(path, fileEntry.Name())
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open %v", filePath)
				for _, oFile := range fileList {
					oFile.Close()
				}
				return nil, err
			}
			fileList = append(fileList, file)
		}
	}
	return fileList, nil
}


func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) error {
	key_id := reply.TaskId

	k_vs := map[string][]string{}

	fileList, err := ReadSpecificFile(key_id, "./")

	if err != nil {
		return err
	}

	// 整理所有的中间文件
	for _, file := range fileList {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			k_vs[kv.Key] = append(k_vs[kv.Key], kv.Value)
		}
		file.Close()
	}

	// 获取所有的键并排序
	var keys []string
	for k := range k_vs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	oname := "mr-out-" + strconv.Itoa(reply.TaskId)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, k_vs[key])
		_, err := fmt.Fprintf(ofile, "%v %v\n", key, output)
		if err != nil {
			return err
		}
	}

	DelFileByReduceId(reply.TaskId, "./")
	return nil
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		replyMsg := CallForTasks()
		if replyMsg == nil {
			fmt.Println("reply failed!, continue")
			time.Sleep(1 * time.Second)
			continue
		}
		switch replyMsg.MsgType {
		case MapTaskAlloc:
			fmt.Printf("MapTaskAlloc %d\n", replyMsg.TaskId)
			err := HandleMapTask(replyMsg, mapf)
			if err == nil {
                CallForReportStatus(MapSuccess, replyMsg.TaskId)
			} else {
				CallForReportStatus(MapFailed, replyMsg.TaskId)
			}
		case ReduceTaskAlloc:
			fmt.Printf("ReduceTaskAlloc: %d\n", replyMsg.TaskId)
			err := HandleReduceTask(replyMsg, reducef)
			if err == nil {
                CallForReportStatus(ReduceSuccess, replyMsg.TaskId)
			} else {
				CallForReportStatus(ReduceFailed, replyMsg.TaskId)
			}
		case Wait:
			fmt.Println("No task at hand, wait")
			time.Sleep(10 * time.Second)
		case Shutdown:
			fmt.Println("All Tasks well done, exit")
			os.Exit(0)
		}
		time.Sleep(time.Second)
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
