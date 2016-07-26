package batch

import (
	"fmt"
	"sync"
	"testing"
)

func TestBatch(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc := make(chan string)
			task := NewTask(10, 5)
			var count int
			go func() {
				for dv := range task.RespCh {
					if dv.Mt == 0 {
						fmt.Println(fmt.Sprintf("id %s,mt %d", dv.Id, dv.Mt))
					}
					count++
				}
				cc <- "1"
			}()

			for i := 0; i < 98; i++ {
				task.Add(fmt.Sprintf("%d", i))
			}
			task.Wait()
			<-cc
			if count != 98 {
				fmt.Println("count ", count)
			}
		}()
	}
	wg.Wait()
}
