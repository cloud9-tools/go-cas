package internal

import "sync"

func Locked(locker sync.Locker, fn func()) {
	locker.Lock()
	defer locker.Unlock()
	fn()
}
