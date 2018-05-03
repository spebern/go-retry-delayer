package retry_delayer

import (
	"container/list"
	"reflect"
	"testing"
	"time"

	"github.com/orcaman/concurrent-map"
)

func TestRemove(t *testing.T) {
	msgGroups := list.New()
	msgGroups.PushFront(msgGroup{
		until: time.Now().Add(60 * time.Second),
		store: cmap.New()})

	store := cmap.New()
	store.Set("123", nil)
	msgGroups.PushFront(msgGroup{
		until: time.Now().Add(30 * time.Second),
		store: store})

	delayer := &delayer{msgGroups: msgGroups}
	delayer.Remove("123")
	if store.Has("123") {
		t.Error("remove msg failed")
	}
}

func TestReplace(t *testing.T) {
	msgGroups := list.New()
	msgGroups.PushFront(msgGroup{
		until: time.Now().Add(60 * time.Second),
		store: cmap.New()})

	store := cmap.New()
	store.Set("123", 1)
	msgGroups.PushFront(msgGroup{
		until: time.Now().Add(30 * time.Second),
		store: store})

	delayer := &delayer{msgGroups: msgGroups}
	delayer.Replace("123", 2)

	v, exists := store.Get("123")
	if !exists || v.(int) != 2 {
		t.Error("replace msg failed")
	}
}

func TestDelay(t *testing.T) {
	delayer := &delayer{
		msgGroups:  list.New(),
		jitter:     10 * time.Millisecond,
		deadlines:  make(chan time.Time, 10),
		retryAfter: 1 * time.Second}
	delayer.Delay("1", 1)

	time.Sleep(5 * time.Millisecond)
	delayer.Delay("2", 2)

	time.Sleep(5 * time.Millisecond)
	delayer.Delay("3", 3)

	time.Sleep(10 * time.Millisecond)
	delayer.Delay("4", 4)

	if delayer.msgGroups.Len() != 3 {
		t.Error("did not create correct number of message groups")
		return
	}

	var msgGroups []msgGroup
	for e := delayer.msgGroups.Front(); e != nil; e = e.Next() {
		msgGroups = append(msgGroups, e.Value.(msgGroup))
	}

	if !msgGroups[0].store.Has("1") {
		t.Error("1. msg not stored correctly")
	}
	if !msgGroups[0].store.Has("2") {
		t.Error("2. msg not stored correctly")
	}
	if !msgGroups[1].store.Has("3") {
		t.Error("3. msg not stored correctly")
	}
	if !msgGroups[2].store.Has("4") {
		t.Error("4. msg not stored correctly")
	}
}

func TestRetryJob(t *testing.T) {
	msgsGroups := list.New()
	store := cmap.New()
	store.Set("1", msg{id: "1", content: 1})
	store.Set("2", msg{id: "2", content: 2})

	deadline := time.Now().Add(5 * time.Second)
	msgsGroups.PushFront(msgGroup{
		store: store,
		until: deadline})

	delayer := &delayer{
		stop:      make(chan struct{}),
		deadlines: make(chan time.Time),
		msgs:      make(chan msg),
		msgGroups: msgsGroups}

	go delayer.retryJob()

	delayer.deadlines <- deadline
	msgCount := 0
	var msgs [2]msg
	timeout := time.After(6 * time.Second)
L:
	for {
		select {
		case msg := <-delayer.msgs:
			if msg.id == "1" {
				msgs[0] = msg
			} else {
				msgs[1] = msg
			}
			msgCount++
			if msgCount == 2 {
				break L
			}
		case <-timeout:
			t.Error("did not receive messages in expected time frame")
			return
		}
	}

	if !(msgs[0].id == "1") {
		t.Error("did not receive 1. message")
	}
	if !(msgs[1].id == "2") {
		t.Error("did not receive 2. message")
	}

	if !(msgs[0].content.(int) == 1) {
		t.Error("content of msgs 1. not correct")
	}
	if !(msgs[1].content.(int) == 2) {
		t.Error("content of msgs 1. not correct")
	}

	delayer.stop <- struct{}{}
}

func TestRun(t *testing.T) {
	msgs := make(chan msg)
	typeToRetry := make(map[reflect.Type]func(interface{}) bool)

	i := 1
	var intRetried bool
	typeToRetry[reflect.TypeOf(i)] = func(v interface{}) bool {
		i := v.(int)
		if i != 1 {
			t.Error("retry with int failed")
		}
		intRetried = true
		return true
	}

	var stringRetried bool
	s := "bold"
	typeToRetry[reflect.TypeOf(s)] = func(v interface{}) bool {
		s := v.(string)
		if s != "bold" {
			t.Error("retry with string failed")
		}
		stringRetried = true
		return true
	}

	w := newWorker(msgs, typeToRetry)
	go w.run()

	msgs <- msg{content: i}
	msgs <- msg{content: s}

	time.Sleep(10 * time.Millisecond)

	if !intRetried {
		t.Error("did not retry int")
	}
	if !stringRetried {
		t.Error("did not retry string")
	}

	w.stop <- struct{}{}
}

func TestNew(t *testing.T) {
	msgToRetry := make(map[interface{}]func(interface{}) bool)
	type msg1 struct{}
	type msg2 struct{}
	msgToRetry[msg1{}] = func(v interface{}) bool {
		return true
	}
	msgToRetry[msg2{}] = func(v interface{}) bool {
		return true
	}

	retryAfter := 2 * time.Second
	jitter := 100 * time.Millisecond
	concurrentRetries := uint(2)
	maxRetries := uint(3)
	delayer := New(retryAfter, jitter, concurrentRetries, maxRetries, msgToRetry).(*delayer)

	if delayer.retryAfter != retryAfter {
		t.Error("didn't initialize retryAfter correctly")
	}
	if delayer.maxRetries != maxRetries {
		t.Error("didn't initialize retryAfter correctly")
	}
	if delayer.jitter != jitter {
		t.Error("didn't initialize jitter correctly")
	}
	if len(delayer.workers) != int(concurrentRetries) {
		t.Error("didn't initialize correct number of workers")
	}
	if delayer.msgGroups == nil {
		t.Error("didn't initialize msgGroups")
	}

	delayer.Stop()
}

func BenchmarkRetryLater(b *testing.B) {
	msgToRetry := make(map[interface{}]func(interface{}) bool)
	msgToRetry[0] = func(v interface{}) bool {
		return true
	}

	delayer := New(time.Second, 5*time.Millisecond, 8, 3, msgToRetry).(*delayer)

	for n := 0; n < b.N; n++ {
		delayer.Delay(string(n), n)
	}
}
