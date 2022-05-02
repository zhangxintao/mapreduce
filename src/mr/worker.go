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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		log.Println("asking for new task")
		response := askForTask()
		if response.TaskType == Map {
			log.Println("get a map")
			outputs := processMap(response.MapTask, mapf)
			completeMap(response.MapTask, outputs)
		}

		if response.TaskType == Reduce {
			log.Println("get a reduce")
			processReduce(response.ReduceTask, reducef)
			completeReduce(response.ReduceTask)
		}
		time.Sleep(time.Second * 10)
	}
}
func processMap(mapResponse MapTaskResponse, mapf func(string, string) []KeyValue) map[int]string {
	filename := mapResponse.FilePath
	log.Println("processing file:", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	sort.Sort(ByKey(intermediate))

	prefix := "mr-" + strconv.Itoa(mapResponse.TaskNumber) + "-"

	output := make(map[int]string)
	i := 0
	for i < len(intermediate) {
		key := ihash(intermediate[i].Key) % mapResponse.NReduce
		outfile := prefix + strconv.Itoa(key)
		log.Println("processing ", intermediate[i], ";processing key:", key, ";writting to file:", outfile)

		file, err := os.OpenFile(outfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
		}
		defer file.Close()

		enc := json.NewEncoder(file)
		enc.Encode(&intermediate[i])
		output[key] = outfile
		i++
	}

	return output
}

func completeMap(mapResponse MapTaskResponse, outputs map[int]string) {
	request := CompleteTaskRequest{}
	response := CompleteTaskResponse{}

	request.TaskType = Map
	request.Map = CompleteMapTaskRequest{MapTaskNum: mapResponse.TaskNumber, OutputFiles: outputs}
	ok := call("Coordinator.CompleteTask", &request, &response)
	if ok {
		log.Println("completed a task", request)
	} else {
		log.Fatalln("complete task failed", request)
	}
}
func processReduce(reduceResponse ReduceTaskResponse, reducef func(string, []string) string) {
	outputfilename := "mr-out-" + strconv.Itoa(reduceResponse.TaskNumber)
	tempoutputfilename := outputfilename + "-tmp"

	intermediate := []KeyValue{}
	for _, filename := range reduceResponse.IntermediateFiles {
		intermediateFile, err := os.Open(filename)
		if err != nil {
			log.Fatalln("cannot open intermedidate file:", filename)
		}

		log.Println("processing file:", filename)
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

	}
	outfile, err := os.OpenFile(tempoutputfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer outfile.Close()
	log.Println("writing to output:", tempoutputfilename)
	i := 0
	sort.Sort(ByKey(intermediate))
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		log.Printf("writing: %v %v source:%v\n", intermediate[i].Key, output, tempoutputfilename)
		fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tempoutputfilename, outputfilename)
	os.Remove(tempoutputfilename)
}

func completeReduce(reduceResponse ReduceTaskResponse) {
	request := CompleteTaskRequest{}
	response := CompleteTaskResponse{}

	request.TaskType = Reduce
	request.Reduce = CompleteReduceTaskRequest{ReduceTastNum: reduceResponse.TaskNumber}
	ok := call("Coordinator.CompleteTask", &request, &response)
	if ok {
		log.Println("completed a reduce task", request)
	} else {
		log.Fatalln("complete reduce task failed", request)
	}
}
func askForTask() AskTaskResponse {
	args := AskTaskReqeust{}

	response := AskTaskResponse{MapTask: MapTaskResponse{}, ReduceTask: ReduceTaskResponse{}}
	ok := call("Coordinator.CoordinateTask", &args, &response)
	if ok {
		log.Println("asked a task - ", response)
	} else {
		log.Fatalln("ask task failed")
	}
	return response
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
