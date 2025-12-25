package votecount

import (
	"math/rand/v2"
	"sync"
	"time"
)

// https://pdos.csail.mit.edu/6.824/notes/condvar.tar.gz
func Main1() {
	count := 0
	finished := 0
	var mu sync.Mutex

	for range 10 {
		go func() {
			vote := requestVote1()
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
		}()
	}

	for {
		mu.Lock()
		if count >= 5 || finished == 10 {
			break
		}
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
	mu.Unlock()
}

func requestVote1() bool {
	time.Sleep(time.Duration(rand.IntN(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
