package batch_get

import (
	"sync"
)

type Response struct {
	Msg interface{}
	Err error
}

type Task struct {
	Batch      int
	ReqCh      chan interface{}
	BatchReqCh chan []interface{}
	RespCh     chan *Response
	wg         sync.WaitGroup
	Cb         func() (interface{}, error)
}

func NewTask(b, workers int, cb func()) *Task {
	t := &Task{
		Batch:  b,
		ReqCh:  make(chan interface{}),
		RespCh: make(chan interface{}),
		Cb:     cb,
	}
	go t.Do()
	for i := 0; i < len(workers); i++ {
		go t.Worker()
	}
	return t
}

func (t *Task) Worker() {
	for req := range t.BatchReqCh {
		ds, err := t.Cb(req)
		t.RespCh <- &Response{ds, err}
		t.wg.Done()
	}
}

type item interface{}

func (t *Task) Do() {
	reqs := []item{}
	for req := range t.ReqCh {
		if len(reqs) >= t.Batch {
			t.wg.Add(1)
			t.BatchReqCh <- reqs
			reqs = []item{}
		}
		reqs = append(reqs, req)
	}
	t.wg.Add(1)
	t.BatchReqCh <- reqs
	close(t.BatchReqCh)
}

func (t *Task) Add(req interface{}) {
	t.ReqCh <- req
}

func (t *Task) Close() {
	close(t.ReqCh)
	t.wg.Wait()
	close(t.RespCh)
}
