package manage

import (
	"math/rand"
	"sync"
	"time"
)

var (
	randStringChars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	randStringMu    = new(sync.Mutex) //protects randStringRand, which isn't threadsafe
	randStringRand  = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func RandString(n int) string {
	b := make([]rune, n)
	l := len(randStringChars)
	randStringMu.Lock()
	for i := range b {
		b[i] = randStringChars[randStringRand.Intn(l)]
	}
	randStringMu.Unlock()
	return string(b)
}

