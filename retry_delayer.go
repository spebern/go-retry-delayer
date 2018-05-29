package retry_delayer

import (
	"container/list"
	"reflect"
	"sync"
	"time"

	"github.com/orcaman/concurrent-map"
)

type RetryDelayer interface {
	Delay(string, interface{})
	Remove(id string)
	Replace(id string, msg interface{})
	ReplaceOrDelay(id string, msg interface{})
	Stop()
}

type msg struct {
	content interface{}
	id      string
	retried uint
}

type worker struct {
	msgs        chan msg
	stop        chan struct{}
	typeToRetry map[reflect.Type]func(interface{}) bool
	delayer     *delayer
}

type delayer struct {
	retryAfter        time.Duration
	jitter            time.Duration
	maxRetries        uint
	concurrentRetries uint
	msgGroups         *list.List
	msgGroupsMu       sync.RWMutex
	stop              chan struct{}
	deadlines         chan time.Time
	msgs              chan msg
	workers           []*worker
}

type msgGroup struct {
	until time.Time
	store cmap.ConcurrentMap
}

func (delayer *delayer) Remove(id string) {
	delayer.msgGroupsMu.RLock()
	for e := delayer.msgGroups.Back(); e != nil; e = e.Prev() {
		store := e.Value.(msgGroup).store
		if store.Has(id) {
			store.Remove(id)
			break
		}
	}
	delayer.msgGroupsMu.RUnlock()
}

func (delayer *delayer) Replace(id string, content interface{}) {
	delayer.msgGroupsMu.RLock()
	for e := delayer.msgGroups.Back(); e != nil; e = e.Prev() {
		store := e.Value.(msgGroup).store
		if store.Has(id) {
			store.Set(id, msg{id: id, content: content})
			break
		}
	}
	delayer.msgGroupsMu.RUnlock()
}

func (delayer *delayer) ReplaceOrDelay(id string, content interface{}) {
	delayer.msgGroupsMu.RLock()
	for e := delayer.msgGroups.Back(); e != nil; e = e.Prev() {
		store := e.Value.(msgGroup).store
		if store.Has(id) {
			store.Set(id, msg{id: id, content: content})
			delayer.msgGroupsMu.RUnlock()
			return
		}
	}
	delayer.msgGroupsMu.RUnlock()
	delayer.Delay(id, content)
}

func (delayer *delayer) Delay(id string, content interface{}) {
	ts := time.Now()
	msg := msg{id: id, content: content}

	delayer.msgGroupsMu.RLock()
	latestMsgGroupE := delayer.msgGroups.Back()
	if latestMsgGroupE == nil || ts.Add(delayer.retryAfter).After(latestMsgGroupE.Value.(msgGroup).until) {
		delayer.msgGroupsMu.RUnlock()
		delayer.msgGroupsMu.Lock()
		if latestMsgGroupE == nil || ts.Add(delayer.retryAfter).After(latestMsgGroupE.Value.(msgGroup).until) {
			deadline := ts.Add(delayer.jitter).Add(delayer.retryAfter)
			delayer.deadlines <- deadline
			newMsgsGroup := msgGroup{
				until: deadline,
				store: cmap.New()}
			newMsgsGroup.store.Set(id, msg)
			delayer.msgGroups.PushBack(newMsgsGroup)
		} else {
			latestMsgGroupE := delayer.msgGroups.Back()
			latestMsgGroupE.Value.(msgGroup).store.Set(id, msg)
		}
		delayer.msgGroupsMu.Unlock()
	} else {
		latestMsgGroupE.Value.(msgGroup).store.Set(id, msg)
		delayer.msgGroupsMu.RUnlock()
	}
}

func (delayer *delayer) retryJob() {
	deadlines := list.New()
	var retryMsgGroup <-chan time.Time
	for {
		select {
		case <-delayer.stop:
			return
		case d := <-delayer.deadlines:
			if deadlines.Len() == 0 {
				retryMsgGroup = time.After(d.Sub(time.Now()))
			} else {
				deadlines.PushBack(d)
			}
		case <-retryMsgGroup:
			delayer.msgGroupsMu.Lock()
			mg := delayer.msgGroups.Remove(delayer.msgGroups.Front()).(msgGroup)
			delayer.msgGroupsMu.Unlock()
			go func() {
				for _, msgV := range mg.store.Items() {
					delayer.msgs <- msgV.(msg)
				}
			}()
			if deadlines.Len() > 0 {
				d := deadlines.Remove(deadlines.Front()).(time.Time)
				retryMsgGroup = time.After(d.Sub(time.Now()))
			}
		}
	}
}

func newWorker(msgs chan msg, typeToRetry map[reflect.Type]func(interface{}) bool) *worker {
	return &worker{
		msgs:        msgs,
		stop:        make(chan struct{}),
		typeToRetry: typeToRetry}
}

func (w *worker) run() {
	for {
		select {
		case <-w.stop:
			return
		case msg := <-w.msgs:
			retry, exists := w.typeToRetry[reflect.TypeOf(msg.content)]
			if !exists {
				panic("missing retry function for msg type")
			}
			if !retry(msg.content) && msg.retried < w.delayer.maxRetries {
				msg.retried++
				w.delayer.Delay(msg.id, msg.content)
			}
		}
	}
}

func (delayer *delayer) Stop() {
	for _, w := range delayer.workers {
		w.stop <- struct{}{}
	}
	delayer.stop <- struct{}{}
}

func New(retryAfter, jitter time.Duration, concurrentRetries, maxRetries uint,
	msgToRetry map[interface{}]func(interface{}) bool) RetryDelayer {
	typeToRetry := make(map[reflect.Type]func(interface{}) bool)
	for msgContent, retry := range msgToRetry {
		typeToRetry[reflect.TypeOf(msgContent)] = retry
	}

	msgs := make(chan msg, concurrentRetries)
	delayer := &delayer{
		msgGroups:         list.New(),
		retryAfter:        retryAfter,
		jitter:            jitter,
		maxRetries:        maxRetries,
		concurrentRetries: concurrentRetries,
		deadlines:         make(chan time.Time),
		msgs:              msgs,
		stop:              make(chan struct{})}

	var workers []*worker
	for i := uint(0); i < concurrentRetries; i++ {
		w := newWorker(msgs, typeToRetry)
		w.delayer = delayer
		go w.run()
		workers = append(workers, w)
	}
	delayer.workers = workers

	go delayer.retryJob()

	return delayer
}
