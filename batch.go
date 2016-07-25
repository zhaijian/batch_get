package batch

import (
	"sync"
	"time"
)

type Response struct {
	Msg interface{}
	Err error
}

type Task struct {
	Batch      int
	ReqCh      chan string
	BatchReqCh chan []string
	wg         sync.WaitGroup
	Cb         func(*BasicDv)
}

func NewTask(b, workers int, cb func(dv *BasicDv)) *Task {
	t := &Task{
		Batch:      b,
		ReqCh:      make(chan string),
		BatchReqCh: make(chan []string),
		Cb:         cb,
	}
	go t.Do()
	for i := 0; i < workers; i++ {
		t.wg.Add(1)
		go t.Worker()
	}
	return t
}

func (t *Task) Worker() {
	for req := range t.BatchReqCh {
		batchGet(req, t.Cb)
	}
	t.wg.Done()
}

func batchGet(arr []string, cb func(dv *BasicDv)) {
	for _, id := range arr {
		cb(&BasicDv{
			Id: id,
			Mt: time.Now().UnixNano(),
		})
	}
}

func (t *Task) Do() {
	reqs := []string{}
	for req := range t.ReqCh {
		if len(reqs) >= t.Batch {
			t.BatchReqCh <- reqs
			reqs = []string{}
		}
		reqs = append(reqs, req)
	}
	if len(reqs) > 0 {
		t.BatchReqCh <- reqs
	}
	close(t.BatchReqCh)
}

func (t *Task) Add(req string) {
	t.ReqCh <- req
}

func (t *Task) Wait() {
	close(t.ReqCh)
	t.wg.Wait()
}

type BasicDv struct {
	Id string
	Mt int64
}
