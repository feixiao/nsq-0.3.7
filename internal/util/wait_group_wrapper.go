package util

import (
	"sync"
)

// 封装sync.WaitGroup，等待goroutine退出
type WaitGroupWrapper struct {
	sync.WaitGroup
}

// 传入调用函数，执行前WaitGroup加一，运行结束减一
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
