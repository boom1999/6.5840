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

const maxTaskTime = 10 // 10 seconds

type MapTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type ReduceTaskState struct {
	beginSecond int64
	workerId    int
}

type Coordinator struct {
	// Your definitions here.
	fileNames   []string
	nReduce     int
	curWorkerId int

	unIssueMapTasks *BlockQueue
	issuedMapTasks  *MapSet
	issuedMapMutex  sync.Mutex

	unIssueReduceTasks *BlockQueue
	issuedReduceTasks  *MapSet
	issuedReduceMutex  sync.Mutex

	// task states
	mapTaskStates    []MapTaskState
	reduceTaskStates []ReduceTaskState

	// status
	mapDone bool
	allDone bool
}

// Your code here -- RPC handlers for the worker to call.

type MapTaskArgs struct {
	// give -1 if no task
	WorkerId int
}
type MapTaskReply struct {
	// worker pass the file name to the os to read
	FileName string

	// marks a unique file id for map task, give -1 if no more file
	FileId int

	// for reduce task
	NReduce int

	// assign the worker id to the task
	WorkerId int

	// assign where this kind of tasks are all done
	// if not or FileId is -1, the worker should wait
	AllDone bool
}

type ReduceTaskArgs struct {
	// give -1 if no task
	WorkerId int
}
type ReduceTaskReply struct {
	RIndex    int
	NReduce   int
	FileCount int
	WorkerId  int
	AllDone   bool
}

type MapTaskJoinArgs struct {
	FileId   int
	WorkerId int
}

type MapTaskJoinReply struct {
	Accept bool
}

type ReduceTaskJoinArgs struct {
	WorkerId int
	RIndex   int
}

type ReduceTaskJoinReply struct {
	Accept bool
}

func mapDoneProcess(reply *MapTaskReply) {
	log.Println("All map tasks are done, start reduce tasks")
	reply.FileId = -1
	reply.AllDone = true
}

func (c *Coordinator) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	if args.WorkerId == -1 {
		reply.WorkerId = c.curWorkerId
		c.curWorkerId++
	} else {
		reply.WorkerId = args.WorkerId
	}
	log.Printf("GiveMapTask: workerId=%d\n", reply.WorkerId)

	c.issuedMapMutex.Lock()

	if c.mapDone {
		c.issuedMapMutex.Unlock()
		mapDoneProcess(reply)
		return nil
	}

	if c.unIssueMapTasks.Size() == 0 && c.issuedMapTasks.Size() == 0 {
		c.issuedMapMutex.Unlock()
		mapDoneProcess(reply)
		c.prepareAllReduceTasks()
		c.mapDone = true
		return nil
	}
	log.Printf("%v unissued map tasks, %v issued map tasks\n", c.unIssueMapTasks.Size(), c.issuedMapTasks.Size())

	// release the lock to allow other workers to issue tasks
	c.issuedMapMutex.Unlock()
	curTime := getNowTimeSecond()
	ret, err := c.unIssueMapTasks.PopBack()
	var fileId int
	if err != nil {
		log.Println("no map task yet, wait...")
		fileId = -1
	} else {
		fileId = ret.(int)
		c.issuedMapMutex.Lock()
		reply.FileName = c.fileNames[fileId]
		c.mapTaskStates[fileId] = MapTaskState{beginSecond: curTime, workerId: reply.WorkerId, fileId: fileId}
		c.issuedMapTasks.Add(fileId)
		c.issuedMapMutex.Unlock()
		log.Printf("Giving map task %v on file %v at second %v\n", fileId, c.fileNames[fileId], curTime)
	}

	reply.FileId = fileId
	reply.AllDone = false
	reply.NReduce = c.nReduce
	return nil

}

func (c *Coordinator) JoinMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	// check current time for whether the task is timeout
	log.Printf("Get join request from worker %v on file %v %v\n", args.WorkerId, args.FileId, c.fileNames[args.FileId])

	c.issuedMapMutex.Lock()

	curTime := getNowTimeSecond()
	taskTime := c.mapTaskStates[args.FileId].beginSecond

	if !c.issuedMapTasks.Contains(args.FileId) {
		log.Printf("Map task %v is not issued, ignoring...\n", args.FileId)
		c.issuedMapMutex.Unlock()
		reply.Accept = false
		return nil
	}

	if c.mapTaskStates[args.FileId].workerId != args.WorkerId {
		log.Printf("Map task %v is not assigned to worker %v, ignoring...\n", args.FileId, args.WorkerId)
		c.issuedMapMutex.Unlock()
		reply.Accept = false
		return nil
	}

	if curTime-taskTime > maxTaskTime {
		log.Println("Map task timeout, put back to queue")
		reply.Accept = false
		c.unIssueMapTasks.PutFront(args.FileId)
	} else {
		log.Println("Map task within max wait time, accepting...")
		reply.Accept = true
		c.issuedMapTasks.Remove(args.FileId)
	}
	c.issuedMapMutex.Unlock()
	return nil
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func (c *Coordinator) prepareAllReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		log.Printf("Putting reduce task %v to channel\n", i)
		c.unIssueReduceTasks.PutFront(i)
	}
}

func (c *Coordinator) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	if args.WorkerId == -1 {
		reply.WorkerId = c.curWorkerId
		c.curWorkerId++
	} else {
		reply.WorkerId = args.WorkerId
	}
	log.Printf("GiveReduceTask: workerId=%d\n", reply.WorkerId)

	c.issuedReduceMutex.Lock()

	if c.unIssueReduceTasks.Size() == 0 && c.issuedReduceTasks.Size() == 0 {
		c.issuedReduceMutex.Unlock()
		log.Println("All reduce tasks are done")
		reply.AllDone = true
		reply.RIndex = -1
		c.allDone = true
		return nil
	}
	log.Printf("%v unissued reduce tasks, %v issued reduce tasks\n", c.unIssueReduceTasks.Size(), c.issuedReduceTasks.Size())

	// release the lock to allow other workers to issue tasks
	c.issuedReduceMutex.Unlock()
	curTime := getNowTimeSecond()
	ret, err := c.unIssueReduceTasks.PopBack()
	var rIndex int
	if err != nil {
		log.Println("no reduce task yet, wait...")
		rIndex = -1
	} else {
		rIndex = ret.(int)
		c.issuedReduceMutex.Lock()
		c.reduceTaskStates[rIndex] = ReduceTaskState{beginSecond: curTime, workerId: args.WorkerId}
		c.issuedReduceTasks.Add(rIndex)
		c.issuedReduceMutex.Unlock()
		log.Printf("Giving reduce task %v at second %v\n", rIndex, curTime)
	}

	reply.RIndex = rIndex
	reply.AllDone = false
	reply.NReduce = c.nReduce
	reply.FileCount = len(c.fileNames)
	return nil
}

func (c *Coordinator) JoinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	// check current time for whether the task is timeout
	log.Printf("Get join request from worker %v on reduce task %v\n", args.WorkerId, args.RIndex)

	c.issuedReduceMutex.Lock()

	curTime := getNowTimeSecond()
	taskTime := c.reduceTaskStates[args.RIndex].beginSecond
	if !c.issuedReduceTasks.Contains(args.RIndex) {
		log.Printf("Reduce task %v is not issued, ignoring...\n", args.RIndex)
		c.issuedReduceMutex.Unlock()
		reply.Accept = false
		return nil
	}

	if c.reduceTaskStates[args.RIndex].workerId != args.WorkerId {
		log.Printf("Reduce task belongs to worker %v not this worker %v, ignoring...\n", c.reduceTaskStates[args.RIndex].workerId, args.WorkerId)
		c.issuedReduceMutex.Unlock()
		reply.Accept = false
		return nil
	}

	if curTime-taskTime > maxTaskTime {
		log.Println("Reduce task timeout, put back to queue")
		reply.Accept = false
		c.unIssueReduceTasks.PutFront(args.RIndex)
	} else {
		log.Println("task within max wait time, accepting...")
		reply.Accept = true
		c.issuedReduceTasks.Remove(args.RIndex)
	}
	c.issuedReduceMutex.Unlock()
	return nil
}

func (m *MapSet) removeTimeoutMapTasks(mapTaskStates []MapTaskState, unIssueMapTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-mapTaskStates[fileId.(int)].beginSecond > maxTaskTime {
				log.Printf("Map task %v timeout, put back to queue\n", fileId)
				unIssueMapTasks.PutFront(fileId)
				m.Remove(fileId)
			}
		}
	}
}

func (m *MapSet) removeTimeoutReduceTasks(reduceTaskStates []ReduceTaskState, unIssueReduceTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-reduceTaskStates[fileId.(int)].beginSecond > maxTaskTime {
				log.Printf("Map task %v timeout, put back to queue\n", fileId)
				unIssueReduceTasks.PutFront(fileId)
				m.Remove(fileId)
			}
		}
	}
}

func (c *Coordinator) removeTimeoutTasks() {
	//log.Println("Removing timeout tasks...")
	c.issuedMapMutex.Lock()
	c.issuedMapTasks.removeTimeoutMapTasks(c.mapTaskStates, c.unIssueMapTasks)
	c.issuedMapMutex.Unlock()

	c.issuedReduceMutex.Lock()
	c.issuedReduceTasks.removeTimeoutReduceTasks(c.reduceTaskStates, c.unIssueReduceTasks)
	c.issuedReduceMutex.Unlock()
}

func (c *Coordinator) loopRemoveTimeoutTasks() {
	for {
		time.Sleep(2 * time.Second)
		c.removeTimeoutTasks()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	if c.allDone {
		log.Println("All tasks are done")
	} else {
		log.Println("Some tasks are still running")
	}
	return c.allDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	log.SetPrefix("Coordinator: ")
	log.Println("Initializing coordinator...")

	// Input Args
	c.fileNames = files
	c.nReduce = nReduce

	c.curWorkerId = 0
	c.mapTaskStates = make([]MapTaskState, len(files))
	c.reduceTaskStates = make([]ReduceTaskState, nReduce)
	c.unIssueMapTasks = NewBlockQueue()
	c.issuedMapTasks = NewMapSet()
	c.unIssueReduceTasks = NewBlockQueue()
	c.issuedReduceTasks = NewMapSet()
	c.mapDone = false
	c.allDone = false

	// start a thread to listens for RPCs from worker.go
	c.server()
	log.Println("Listening for workers...")

	// start a thread to remove timeout tasks
	go c.loopRemoveTimeoutTasks()

	// send the tasks
	log.Printf("Putting %v map tasks to channel in total\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("Putting map task %v to channel\n", i)
		c.unIssueMapTasks.PutFront(i)
	}

	return &c
}
