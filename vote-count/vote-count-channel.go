package votecount

import (
	"math/rand/v2"
	"time"
)

// https://pdos.csail.mit.edu/6.824/notes/condvar.tar.gz
func Main3() {
	count := 0
	ch := make(chan bool)

	for range 10 {
		go func() {
			ch <- requestVote()
		}()
	}
	for range 10 {
		v := <-ch
		if v {
			count += 1
		}
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.IntN(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
