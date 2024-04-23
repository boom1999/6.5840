package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type AWorker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	// false for map, true for reduce
	mapOrReduce bool

	// false for not done, true for done and must exit
	allDone bool

	workerId int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (worker *AWorker) logPrintf(format string, v ...interface{}) {
	log.Printf("Worker %d: "+format, append([]interface{}{worker.workerId}, v...)...)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}

const pathPrefix = "./"

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := AWorker{mapf: mapf, reducef: reducef, mapOrReduce: false, allDone: false, workerId: -1}
	worker.logPrintf("Intialize worker %v\n", worker.workerId)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for !worker.allDone {
		worker.process()
	}
	worker.logPrintf("All tasks are done, exit...\n")
}

func (worker *AWorker) askMapTask() *MapTaskReply {
	// declare an argument structure and reply structure.
	args := MapTaskArgs{WorkerId: worker.workerId}
	reply := MapTaskReply{}

	worker.logPrintf("Asking map task...\n")
	ok := call("Coordinator.GiveMapTask", &args, &reply)
	if !ok {
		worker.logPrintf("askMapTask failed\n")
	}

	worker.workerId = reply.WorkerId

	if reply.FileId == -1 {
		if reply.AllDone {
			worker.logPrintf("All map tasks are done\n")
			return nil
		} else {
			worker.logPrintf("No map task available\n")
			return &reply
		}
	}
	worker.logPrintf("Got map task %v on file %v\n", reply.FileId, reply.FileName)
	return &reply
}

func makeIntermediateFromFile(fileName string, mapf func(string, string) []KeyValue) []KeyValue {
	path := fileName
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", path)
	}
	file.Close()
	kva := mapf(path, string(content))
	return kva
}

func (worker *AWorker) writeToFiles(fileId int, nReduce int, intermediate []KeyValue) {
	kvas := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		index := keyReduceIndex(kv.Key, nReduce)
		kvas[index] = append(kvas[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp(pathPrefix, "mrtemp")
		if err != nil {
			worker.logPrintf("cannot create tempfile for %v\n", i)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvas[i] {
			if err := enc.Encode(&kv); err != nil {
				worker.logPrintf("cannot encode %v\n", kv)
			}
		}
		tempFile.Close()
		outName := fmt.Sprintf("mr-%d-%d", fileId, i)
		err = os.Rename(tempFile.Name(), outName)
		if err != nil {
			worker.logPrintf("Rename tempFile %v failed \n", outName)
		}
	}
}

func (worker *AWorker) joinMapTask(fileId int) {
	args := MapTaskJoinArgs{WorkerId: worker.workerId, FileId: fileId}
	reply := MapTaskJoinReply{}

	call("Coordinator.JoinMapTask", &args, &reply)

	if reply.Accept {
		worker.logPrintf("JoinMapTask success\n")
	} else {
		worker.logPrintf("JoinMapTask failed\n")
	}
}

func (worker *AWorker) executeMap(reply *MapTaskReply) {
	// execute map task
	intermediate := makeIntermediateFromFile(reply.FileName, worker.mapf)
	// write intermediate to files
	worker.writeToFiles(reply.FileId, reply.NReduce, intermediate)
	worker.joinMapTask(reply.FileId)
}

func (worker *AWorker) askReduceTask() *ReduceTaskReply {
	// declare an argument structure and reply structure.
	args := ReduceTaskArgs{WorkerId: worker.workerId}
	reply := ReduceTaskReply{}

	ok := call("Coordinator.GiveReduceTask", &args, &reply)
	if !ok {
		worker.logPrintf("askReduceTask failed\n")
	}

	worker.workerId = reply.WorkerId

	if reply.RIndex == -1 {
		if reply.AllDone {
			worker.logPrintf("All reduce tasks are done, try to terminate worke\n")
			return nil
		} else {
			worker.logPrintf("No reduce task available\n")
			return &reply
		}
	}
	worker.logPrintf("Got reduce task %v\n", reply.RIndex)
	return &reply
}

func (worker *AWorker) executeReduce(reply *ReduceTaskReply) {
	outName := fmt.Sprintf("mr-out-%v", reply.RIndex)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.FileCount; i++ {
		worker.logPrintf("Reading intermediate files on cluster %v\n", i)
		intermediate = append(intermediate, readIntermediateFiles(i, reply.RIndex)...)
	}
	worker.logPrintf("Total intermediate size: %v\n", len(intermediate))
	tempfile, err := os.CreateTemp(pathPrefix, "mrtemp")
	if err != nil {
		worker.logPrintf("cannot create tempfile for %v\n", outName)
	}
	reduceKVSlice(intermediate, worker.reducef, tempfile)
	tempfile.Close()
	err = os.Rename(tempfile.Name(), outName)
	if err != nil {
		worker.logPrintf("cannot rename tempfile for %v\n", outName)
	}
	worker.joinReduceTask(reply.RIndex)
}

func reduceKVSlice(intermediate []KeyValue, reducef func(string, []string) string, ofile io.Writer) {
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && (intermediate)[j].Key == (intermediate)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (intermediate)[k].Value)
		}
		output := reducef((intermediate)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func readIntermediateFiles(fileId int, rIndex int) []KeyValue {
	fileName := fmt.Sprintf("mr-%v-%v", fileId, rIndex)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	dec := json.NewDecoder(file)
	kva := make([]KeyValue, 0)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	return kva
}

func (worker *AWorker) joinReduceTask(rIndex int) {
	args := ReduceTaskJoinArgs{WorkerId: worker.workerId, RIndex: rIndex}
	reply := ReduceTaskJoinReply{}
	call("Coordinator.JoinReduceTask", &args, &reply)
	if reply.Accept {
		worker.logPrintf("JoinReduceTask success\n")
	} else {
		worker.logPrintf("JoinReduceTask failed\n")
	}
}

func (worker *AWorker) process() {
	if !worker.mapOrReduce {
		// process map task
		reply := worker.askMapTask()
		if reply != nil {
			if reply.FileId == -1 {
				// no available map task, wait for a little while
				n := randomSleep()
				worker.logPrintf("No map task available, worker %v waiting %v ms\n", worker.workerId, n)
				return
			} else {
				worker.executeMap(reply)
			}
		} else {
			worker.logPrintf("No map task available, turn to reduce task\n")
			worker.mapOrReduce = true
		}
	}
	if worker.mapOrReduce {
		// process reduce task
		reply := worker.askReduceTask()
		if reply != nil {
			if reply.RIndex == -1 {
				// no available reduce task, wait for a little while
				n := randomSleep()
				worker.logPrintf("No reduce task available, worker %v waiting %v ms\n", worker.workerId, n)
				return
			} else {
				worker.executeReduce(reply)
			}
		} else {
			// all done
			worker.allDone = true
			return // exit
		}
	}
}

// for waiting a random time when no task available
func randomSleep() int {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(100)
	time.Sleep(time.Duration(n) * time.Millisecond)
	return n
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

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
