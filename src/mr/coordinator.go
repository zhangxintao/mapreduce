package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Idle       = 0
	InProgress = 1
	Completed  = 2
	Failed     = 3
)

type MapTask struct {
	TaskNumber int
	Status     int
	FilePath   string
	StartedAt  time.Time
}

type ReduceTask struct {
	TaskNumber        int
	Status            int
	IntermediateFiles []string
	StartedAt         time.Time
}

type Coordinator struct {
	NReduce     int
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	mu          sync.Mutex
}

func (c *Coordinator) CoordinateTask(request *AskTaskReqeust, response *AskTaskResponse) error {
	if tryAssignMapTask(c, response) {
		return nil
	}

	tryAssignReduceTask(c, response)
	return nil
}

func (c *Coordinator) CompleteTask(request *CompleteTaskRequest, responsee *CompleteTaskResponse) error {
	if request.TaskType == Map {
		completeMapTask(c, request.Map)
	}

	if request.TaskType == Reduce {
		completeRedudeTask(c, request.Reduce)
	}

	return nil
}

func completeMapTask(c *Coordinator, mapRequest CompleteMapTaskRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Println("start completing map task:", mapRequest)
	log.Println("adding output files")
	for key, element := range mapRequest.OutputFiles {
		c.ReduceTasks[key].Status = Idle
		c.ReduceTasks[key].IntermediateFiles = append(c.ReduceTasks[key].IntermediateFiles, element)
	}
	log.Println("completing output files")
	c.MapTasks[mapRequest.MapTaskNum].Status = Completed
}

func completeRedudeTask(c *Coordinator, reduceRequest CompleteReduceTaskRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Println("completing reduce task:", reduceRequest)
	c.ReduceTasks[reduceRequest.ReduceTastNum].Status = Completed
}
func tryAssignMapTask(c *Coordinator, response *AskTaskResponse) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Println("trying to assign a map task")
	i := 0
	for i < len(c.MapTasks) {
		if c.MapTasks[i].Status == Idle {
			response.TaskType = Map
			response.MapTask = MapTaskResponse{TaskNumber: c.MapTasks[i].TaskNumber, FilePath: c.MapTasks[i].FilePath, NReduce: c.NReduce}
			c.MapTasks[i].Status = InProgress
			c.MapTasks[i].StartedAt = time.Now().UTC()
			log.Println("after assign map task", c.MapTasks)
			return true
		}
		i++
	}
	return false
}

func tryAssignReduceTask(c *Coordinator, response *AskTaskResponse) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Println("trying to assign a reduce task")
	i := 0
	for i < len(c.MapTasks) {
		if c.MapTasks[i].Status != Completed {
			log.Println("map task is still in progress:", c.MapTasks[i])
			return false
		}
		i++
	}

	j := 0
	for j < len(c.ReduceTasks) {
		if c.ReduceTasks[j].Status == Idle {
			log.Println("assigning reduce task:", c.ReduceTasks[j].TaskNumber)
			response.TaskType = Reduce
			response.ReduceTask = ReduceTaskResponse{TaskNumber: c.ReduceTasks[j].TaskNumber, IntermediateFiles: c.ReduceTasks[j].IntermediateFiles}
			c.ReduceTasks[j].Status = InProgress
			c.ReduceTasks[j].StartedAt = time.Now().UTC()
			return true
		}
		j++
	}

	return false
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
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := true

	i := 0
	for i < len(c.MapTasks) {
		if c.MapTasks[i].Status == InProgress {
			duration := time.Now().UTC().Sub(c.MapTasks[i].StartedAt).Seconds()
			if duration > 10 {
				c.MapTasks[i].Status = Idle
			}
		}
		i++
	}

	j := 0
	for j < len(c.ReduceTasks) {
		if c.ReduceTasks[j].Status == InProgress {
			duration := time.Now().UTC().Sub(c.ReduceTasks[j].StartedAt).Seconds()
			if duration > 10 {
				c.ReduceTasks[j].Status = Idle
			}
		}
		j++
	}

	for _, maptask := range c.MapTasks {
		if maptask.Status != Completed {
			return false
		}
	}

	for _, reducetask := range c.ReduceTasks {
		if reducetask.Status != Completed {
			return false
		}
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("initiating coordinator with param", files, nReduce)

	c := Coordinator{}
	c.NReduce = nReduce
	c.MapTasks = []MapTask{}
	c.ReduceTasks = []ReduceTask{}

	if len(files) > 0 {
		log.Println("receive more than 0 files input, start processing")
		for index, path := range files {
			log.Println("processing ", path)
			c.MapTasks = append(c.MapTasks, MapTask{TaskNumber: index, Status: Idle, FilePath: path})
		}

		for i := 0; i < nReduce; i++ {
			log.Println("initiating reduce task ", i)
			c.ReduceTasks = append(c.ReduceTasks, ReduceTask{TaskNumber: i, Status: Completed, IntermediateFiles: []string{}})
		}
	}

	log.Println("initiation done, map:", c.MapTasks, ", reduce:", c.ReduceTasks)
	c.server()
	return &c
}
