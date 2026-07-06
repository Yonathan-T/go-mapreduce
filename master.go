package mapreduce

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Master struct {
	inputs       []string
	mapFn        MapFunc
	reduceFn     ReduceFunc
	numReduce    int

	mapTasks    []MapTask
	mapStatus   []TaskStatus

	reduceTasks    []ReduceTask
	reduceStatus   []TaskStatus

	intermediate map[string][]string
	outputs      []string

	mu           sync.Mutex
	allMapDone   bool
	allReduceDone bool
}

func NewMaster(inputs []string, numReduce int, mapFn MapFunc, reduceFn ReduceFunc) *Master {
	return &Master{
		inputs:    inputs,
		mapFn:     mapFn,
		reduceFn:  reduceFn,
		numReduce: numReduce,
	}
}

func (m *Master) Run(numWorkers int) {
	m.intermediate = make(map[string][]string)
	m.outputs = []string{}

	m.mapTasks = make([]MapTask, len(m.inputs))
	for i, input := range m.inputs {
		m.mapTasks[i] = MapTask{ID: i, Input: input}
	}
	m.mapStatus = make([]TaskStatus, len(m.inputs))
	m.allMapDone = false
	m.allReduceDone = false

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.runWorker(id)
		}(i)
	}
	wg.Wait()
}

func (m *Master) runWorker(id int) {
	for {
		task := m.RequestTask()
		switch task.Type {
		case TaskMap:
			kvs := m.mapFn(fmt.Sprintf("doc-%d", task.Map.ID), task.Map.Input)
			m.ReportMapDone(task.Map.ID, kvs)
		case TaskReduce:
			vals := m.intermediate[task.Reduce.Key]
			result := m.reduceFn(task.Reduce.Key, vals)
			m.ReportReduceDone(task.Reduce.ID, result)
		case TaskNone:
			time.Sleep(50 * time.Millisecond)
		case TaskExit:
			return
		}
	}
}

func (m *Master) RequestTask() Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.mapStatus {
		if m.mapStatus[i] == Idle {
			m.mapStatus[i] = InProgress
			return Task{Type: TaskMap, Map: m.mapTasks[i]}
		}
	}

	if !m.allMapDone {
		if m.allMapsCompleted() {
			m.allMapDone = true
			m.initReduceTasks()
		} else {
			return Task{Type: TaskNone}
		}
	}

	for i := range m.reduceStatus {
		if m.reduceStatus[i] == Idle {
			m.reduceStatus[i] = InProgress
			return Task{Type: TaskReduce, Reduce: m.reduceTasks[i]}
		}
	}

	if m.allReduceDone || m.allReducesCompleted() {
		m.allReduceDone = true
		return Task{Type: TaskExit}
	}

	return Task{Type: TaskNone}
}

func (m *Master) allMapsCompleted() bool {
	for _, s := range m.mapStatus {
		if s != Completed {
			return false
		}
	}
	return true
}

func (m *Master) allReducesCompleted() bool {
	for _, s := range m.reduceStatus {
		if s != Completed {
			return false
		}
	}
	return true
}

func (m *Master) initReduceTasks() {
	keySet := make(map[string]struct{})
	for k := range m.intermediate {
		keySet[k] = struct{}{}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	m.reduceTasks = make([]ReduceTask, len(keys))
	for i, k := range keys {
		m.reduceTasks[i] = ReduceTask{ID: i, Key: k}
	}
	m.reduceStatus = make([]TaskStatus, len(keys))
}

func (m *Master) ReportMapDone(id int, kvs []KeyValue) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, kv := range kvs {
		m.intermediate[kv.Key] = append(m.intermediate[kv.Key], kv.Value)
	}
	m.mapStatus[id] = Completed
}

func (m *Master) ReportReduceDone(id int, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.reduceTasks[id].Key
	m.outputs = append(m.outputs, fmt.Sprintf("%s %s", key, result))
	m.reduceStatus[id] = Completed
}

func (m *Master) Output() string {
	sort.Strings(m.outputs)
	return strings.Join(m.outputs, "\n")
}
