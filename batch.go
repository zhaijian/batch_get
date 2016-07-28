package batch

import (
	"sync"
	"time"
)

type Task struct {
	Batch      int
	ReqArray   []string
	RespCh     chan *BasicDv
	BatchReqCh chan []string
	wg         sync.WaitGroup
	Cb         func(*BasicDv)
}

func NewTask(b, workers int) *Task {
	t := &Task{
		Batch:      b,
		ReqArray:   []string{},
		RespCh:     make(chan *BasicDv),
		BatchReqCh: make(chan []string),
	}
	for i := 0; i < workers; i++ {
		go t.Worker()
	}
	return t
}

func (t *Task) Worker() {
	for req := range t.BatchReqCh {
		dvs := batchGet(req)
		for _, dv := range dvs {
			t.RespCh <- dv
		}
		t.wg.Done()
	}
}

func batchGet(arr []string) (dvs []*BasicDv) {
	for _, id := range arr {
		dvs = append(dvs, &BasicDv{
			Id: id,
			Mt: time.Now().UnixNano(),
		})
	}
	return
}

func (t *Task) Add(req string) {
	if len(t.ReqArray) >= t.Batch {
		t.enqCh(t.ReqArray)
	}
	t.ReqArray = append(t.ReqArray, req)
}

func (t *Task) enqCh(arr []string) {
	t.wg.Add(1)
	t.BatchReqCh <- t.ReqArray
	t.ReqArray = []string{}
}

func (t *Task) Wait() {
	if len(t.ReqArray) > 0 {
		t.enqCh(t.ReqArray)
	}

	t.wg.Wait()
	close(t.RespCh)
	close(t.BatchReqCh)
}

type BasicDv struct {
	Id string
	Mt int64
}
