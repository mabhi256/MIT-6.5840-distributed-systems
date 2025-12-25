package votecount

import (
	"math/rand/v2"
	"sync"
	"time"
)

// https://pdos.csail.mit.edu/6.824/notes/condvar.tar.gz
func Main2() {
	count := 0
	finished := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for range 10 {
		go func() {
			vote := requestVote2()
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
			cond.Broadcast()
		}()
	}

	mu.Lock()
	for count < 5 && finished != 10 {
		cond.Wait()
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
	mu.Unlock()
}

func requestVote2() bool {
	time.Sleep(time.Duration(rand.IntN(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
