package batch

import (
	"fmt"
	"testing"
)

func callback(dv *BasicDv) {
	fmt.Println(fmt.Sprintf("id %s,mt %d", dv.Id, dv.Mt))
}

func TestBatch(t *testing.T) {
	task := NewTask(10, 5, func(dv *BasicDv) {
		callback(dv)
	})
	for i := 0; i < 101; i++ {
		task.Add(fmt.Sprintf("%d", i))
	}
	task.Wait()
}
