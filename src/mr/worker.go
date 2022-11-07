package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const (
	MapTask    int = iota // 任务类型为Map
	ReduceTask            // 任务类型为Reduce
	Waiting               // 等待，暂时没有任务
	Exit                  // Worker退出
)

// 任务的信息，RPC的reply
type Task struct {
	TaskType  int      // 任务类型，是map还是reduce
	TaskId    int      // 任务Id
	ReduceNum int      // 作为Reducer的Worker数量
	InputFile []string // 输入文件
}

// 请求一个任务，不需要参数
type Taskargs struct{}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for GetTaskFlag := true; GetTaskFlag; {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMap(mapf, task)
				CallDone(task)
			}
		case ReduceTask:
			{
				DoReduce(reducef, task)
				CallDone(task)
			}
		case Waiting:
			{
				fmt.Printf("There is no task to do, please waiting!\n")
				time.Sleep(time.Second)
			}
		case Exit:
			{
				fmt.Printf("Worker %d terminated", task.TaskId)
				GetTaskFlag = false
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	// fmt.Printf("%s\n", rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func GetTask() *Task {
	args := Taskargs{}
	reply := Task{}
	ok := call("Coordinator.SendTask", &args, &reply)
	if ok {
		fmt.Printf("Request maptask rpc get response!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func DoMap(mapf func(string, string) []KeyValue, task *Task) {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []KeyValue{}
	// fmt.Println(len(task.InputFile))
	inputFile := task.InputFile[0]
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %v", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}
	file.Close()
	kva := mapf(inputFile, string(content))
	intermediate = append(intermediate, kva...)
	// 上述过程借鉴mrsequential.go，获取了中间键值对

	reduceNum := task.ReduceNum
	hashkv := make([][]KeyValue, reduceNum)

	//进行hash, 分割成reduceNum个中间结果
	for _, kv := range intermediate {
		hashkv[ihash(kv.Key)%reduceNum] = append(hashkv[ihash(kv.Key)%reduceNum], kv)
	}

	//使用JSON将hashkv保存到临时文件中
	for i := 0; i < reduceNum; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashkv[i] {
			if err := enc.Encode(&kv); err != nil {
				fmt.Printf("%s Encoding Error!\n", oname)
			}
		}
		ofile.Close()
	}
}

func DoReduce(reducef func(string, []string) string, task *Task) {
	id := task.TaskId
	intermediate := shuffle(task.InputFile)
	path, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(path, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", id)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func CallDone(f *Task) Task {
	args := f
	reply := Task{}
	ok := call("Coordinator.MapFinished", &args, &reply)

	if ok {
		fmt.Printf("Coordinator get the message of map task success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}
