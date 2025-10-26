package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Task struct {
	Type TaskType
	// For map task, id is from 0 to len(input files) - 1
	// For reduce task, id is from 0 to NReduce - 1
	ID int
}
type Coordinator struct {
	// Your definitions here.
	mutex   sync.Mutex
	nReduce int
	files   []string

	// if pending and inProgress are empty, mapTask is done
	pendingMapTasks    []Task
	inProgressMapTasks map[int]time.Time
	completedMapTasks  map[int]bool

	pendingReduceTasks    []Task
	inProgressReduceTasks map[int]time.Time
	completedReduceTasks  map[int]bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// c.mutex.Lock()
	// defer c.mutex.Unlock()

	fmt.Printf("=== GetTask CALLED ===\n")
	c.mutex.Lock()
	defer func() {
		c.mutex.Unlock()
		fmt.Printf("=== GetTask RETURN === pending=%d, inProgress=%d\n",
			len(c.pendingMapTasks), len(c.inProgressMapTasks))
	}()

	// check and reassign slow task
	now := time.Now()
	// check and reassign slow map task
	for taskID, assignedTime := range c.inProgressMapTasks {
		if now.Sub(assignedTime) > 10*time.Second && !c.completedMapTasks[taskID] {
			fmt.Printf("Map Task %d time out，reallocate!\n", taskID)
			delete(c.inProgressMapTasks, taskID)
			c.pendingMapTasks = append(c.pendingMapTasks, Task{Type: MapTask, ID: taskID})
		}
	}
	// check and reassign slow reduce task
	for taskID, assignedTime := range c.inProgressReduceTasks {
		if now.Sub(assignedTime) > 10*time.Second && !c.completedReduceTasks[taskID] {
			fmt.Printf("Reduce Task %d time out，reallocate!\n", taskID)
			delete(c.inProgressReduceTasks, taskID)
			c.pendingReduceTasks = append(c.pendingReduceTasks, Task{Type: ReduceTask, ID: taskID})
		}
	}

	if len(c.pendingMapTasks) > 0 {
		task := c.pendingMapTasks[0]
		c.pendingMapTasks = c.pendingMapTasks[1:]

		c.inProgressMapTasks[task.ID] = time.Now()

		reply.TaskId = task.ID
		reply.TaskType = MapTask
		reply.NReduce = c.nReduce
		reply.FilePath = c.files[task.ID]
		// fmt.Println("allocate a map Task")
		return nil
	}

	if len(c.pendingMapTasks) == 0 && len(c.inProgressMapTasks) == 0 && len(c.pendingReduceTasks) > 0 {
		reply.NMap = len(c.files)
		task := c.pendingReduceTasks[0]
		c.pendingReduceTasks = c.pendingReduceTasks[1:]

		c.inProgressReduceTasks[task.ID] = time.Now()

		reply.TaskId = task.ID
		reply.TaskType = ReduceTask
		// fmt.Println("return a reduce Task")
		fmt.Printf("分配Reduce任务 %d, inProgressReduceTasks长度: %d\n",
			task.ID, len(c.inProgressReduceTasks))
		return nil
	}

	if len(c.pendingMapTasks) == 0 && len(c.inProgressMapTasks) == 0 && len(c.pendingReduceTasks) == 0 && len(c.inProgressReduceTasks) == 0 {
		reply.TaskType = Exit
		fmt.Println("return a exit Task")
		return nil
	}

	fmt.Println("return a wait Task")
	reply.TaskType = Wait
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch args.TaskType {
	case MapTask:
		c.completedMapTasks[args.TaskId] = true
		delete(c.inProgressMapTasks, args.TaskId)
	case ReduceTask:
		c.completedReduceTasks[args.TaskId] = true
		delete(c.inProgressReduceTasks, args.TaskId)
	default:
		panic("unhandled default case")
	}
	return nil
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
	// l, e := net.Listen("tcp", ":1234")
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := false

	// Your code here.

	// coordinator may exit before worker
	// work will get err then exit when call GetTask
	if len(c.pendingMapTasks) == 0 && len(c.inProgressMapTasks) == 0 && len(c.pendingReduceTasks) == 0 && len(c.inProgressReduceTasks) == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, mutex: sync.Mutex{},
		pendingMapTasks: make([]Task, len(files)), inProgressMapTasks: make(map[int]time.Time, len(files)), completedMapTasks: make(map[int]bool, len(files)),
		pendingReduceTasks: make([]Task, nReduce), inProgressReduceTasks: make(map[int]time.Time, nReduce), completedReduceTasks: make(map[int]bool, nReduce)}

	// Your code here.
	for i := 0; i < len(c.files); i++ {
		c.pendingMapTasks[i] = Task{Type: MapTask, ID: i}
	}

	for i := 0; i < nReduce; i++ {
		c.pendingReduceTasks[i] = Task{Type: ReduceTask, ID: i}
	}

	c.server()
	return &c
}
