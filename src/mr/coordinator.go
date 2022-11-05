package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 全局的锁
var (
	mu sync.Mutex
)

// worker 状态
const (
	WorkingState int = iota
	WaitingState
	DoneState
)

//阶段类型
const (
	MapPhase int = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	// Your definitions here.
	ReduceNum         int                // 作为Reducer的Worker数量
	Taskid            int                // 用于生成worker的TaskId
	InputFiles        []string           // 输入文件
	Phase             int                // 阶段（map, reduce, done）
	TaskChannelMap    chan *Task         // map channel
	TaskChannelReduce chan *Task         // reduce channel
	TaskMaps          map[int]*TaskState // 保存Task信息
}

type TaskState struct {
	State     int       // 任务的状态
	StartTime time.Time // 任务的开始时间，为crash做准备
	TaskAdr   *Task     // 传入任务的指针
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
	mu.Lock()
	defer mu.Unlock()
	if c.Phase == DonePhase {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
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
		ReduceNum:         nReduce,
		InputFiles:        files,
		Phase:             MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		// TaskHandles: TaskHandles{
		// 	make(map[int]*TaskInfo, len(files)+nReduce),
		// },
		TaskMaps: make(map[int]*TaskState, len(files)+nReduce),
	}

	// Your code here.

	c.MakeMapTasks()

	c.server()

	go c.CrashDetector()

	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.Phase == DonePhase {
			mu.Unlock()
			break
		}

		for _, v := range c.TaskMaps {
			if v.State == WorkingState && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))

				switch v.TaskAdr.TaskType {
				case MapPhase:
					{
						v.State = WaitingState
						c.TaskChannelMap <- v.TaskAdr
					}
				case ReducePhase:
					{
						v.State = WaitingState
						c.TaskChannelReduce <- v.TaskAdr
					}
				}
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) MakeMapTasks() {
	for _, v := range c.InputFiles {
		id := c.GenerateTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			InputFile: []string{v},
		}
		// c.TaskHandles.TaskInfoMap[id] = &TaskInfo{
		// 	State: WaitingState,
		// 	// TaskAdr: &task,
		// }
		_, ok := c.TaskMaps[id]
		if ok == true {
			fmt.Printf("contains task which id = %d", id)
			return
		} else {
			c.TaskMaps[id] = &TaskState{
				State:     WaitingState,
				StartTime: time.Now(),
				TaskAdr:   &task,
			}
		}
		// fmt.Printf("Make a map task\n")
		c.TaskChannelMap <- &task
	}

}

func (c *Coordinator) MakeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.GenerateTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			InputFile: ChooseReduceFiles(i),
		}
		_, ok := c.TaskMaps[id]
		if ok == true {
			fmt.Printf("contains task which id = %d", id)
			return
		} else {
			c.TaskMaps[id] = &TaskState{
				State:   WaitingState,
				TaskAdr: &task,
			}
		}
		c.TaskChannelReduce <- &task
	}
}

func ChooseReduceFiles(reduceIndex int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceIndex)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) GenerateTaskId() int {
	res := c.Taskid
	c.Taskid++
	return res
}

func (c *Coordinator) SendTask(args *Taskargs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	//判断阶段
	switch c.Phase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				taskState, ok := c.TaskMaps[c.Taskid]
				if !ok || taskState.State != WaitingState {
					fmt.Printf("The Map Task %d is running or there is no this task!", c.Taskid)
				}
				c.TaskMaps[c.Taskid].State = WorkingState
			} else {
				reply.TaskType = Waiting
				if c.CheckTaskAllDone() {
					c.ToNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				taskState, ok := c.TaskMaps[c.Taskid]
				if !ok || taskState.State != WaitingState {
					fmt.Printf("The Reduce Task %d is running or there is no this task!", c.Taskid)
				}
				c.TaskMaps[c.Taskid].State = WorkingState
			} else {
				reply.TaskType = Waiting
				if c.CheckTaskAllDone() {
					c.ToNextPhase()
				}
				return nil
			}
		}
	case DonePhase:
		{
			reply.TaskType = Exit
		}
	default:
		{
			panic("Undefined phase!")
		}
	}
	return nil
}

func (c *Coordinator) ToNextPhase() {
	if c.Phase == MapPhase {
		c.MakeReduceTasks()
		c.Phase = ReducePhase
	} else if c.Phase == ReducePhase {
		c.Phase = DonePhase
	}
}

func (c *Coordinator) CheckTaskAllDone() bool {
	var (
		MapDownNum      int = 0
		MapUnDoneNum    int = 0
		ReduceDoneNum   int = 0
		ReduceUnDoneNum int = 0
	)

	for _, taskState := range c.TaskMaps {
		if taskState.TaskAdr.TaskType == MapTask {
			if taskState.State == DoneState {
				MapDownNum++
			} else {
				MapUnDoneNum++
			}
		} else if taskState.TaskAdr.TaskType == ReduceTask {
			if taskState.State == DoneState {
				ReduceDoneNum++
			} else {
				ReduceUnDoneNum++
			}
		}
	}

	if MapDownNum > 0 && MapUnDoneNum == 0 && ReduceDoneNum == 0 && ReduceUnDoneNum == 0 {
		return true
	} else if ReduceDoneNum > 0 && ReduceUnDoneNum == 0 {
		return true
	}

	return false
}

func (c *Coordinator) MapFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		{
			taskState, ok := c.TaskMaps[args.TaskId]
			if ok && taskState.State == WorkingState {
				c.TaskMaps[args.TaskId].State = DoneState
			} else {
				fmt.Printf("Map task Id[%d] is finished,already ! or Maybe you have be crashed?!\n", args.TaskId)
			}
		}
	case ReduceTask:
		{
			taskState, ok := c.TaskMaps[args.TaskId]
			if ok && taskState.State == WorkingState {
				c.TaskMaps[args.TaskId].State = DoneState
			} else {
				fmt.Printf("Reduce task Id[%d] is finished,already ! or Maybe you have be crashed?!\n", args.TaskId)
			}
		}
	default:
		panic("The task type undefined ! ! !")
	}
	return nil
}
