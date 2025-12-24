# Hints

<https://pdos.csail.mit.edu/6.824/notes/gopattern.pdf>

---

- Use the race detector, for development and even production.
- Convert data state into code state when it makes programs clearer.
- Convert mutexes into goroutines when it makes programs clearer.
- Use additional goroutines to hold additional code state.
- Use goroutines to let independent concerns run independently.

---

- Consider the effect of slow goroutines.
- Know why and when each communication will proceed.
- Know why and when each goroutine will exit.
- Type Ctrl-\ to kill a program and dump all its goroutine stacks.
- Use the HTTP server’s /debug/pprof/goroutine to inspect live goroutine stacks.

---

- Use a buffered channel as a concurrent blocking queue.
- Think carefully before introducing unbounded queuing.
- Close a channel to signal that no more values will be sent.

---

- Stop timers you don’t need.
- Prefer defer for unlocking mutexes.

---

- Use a mutex if that is the clearest way to write the code.
- Use a goto if that is the clearest way to write the code.
- Use goroutines, channels, and mutexes together if that is the clearest way to write the code
