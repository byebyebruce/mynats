package mynats

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/alecthomas/log4go"
	"github.com/golang/protobuf/proto"
)

func init() {
	log4go.Close()
}

func createTestDispatcher() *Dispatcher {
	cfg := Config{
		Cluster:       "test",
		Server:        "nats://127.0.0.1:4222",
		ReconnectWait: 5,
		MaxReconnects: 1000,
	}
	dis, err := NewDispatcher(&cfg, "Test_Dispatcher", 10)
	if nil != err {
		panic(err)
	}
	return dis
}

func TestNatsDispatcher_Notify(t *testing.T) {

	postfix := "fdasfdas"

	cb1 := func(pb *InnerMessage, reply string, err string) {
		fmt.Println("1 pb", pb, "reply", reply, "err", err)
	}
	dis := createTestDispatcher()
	defer dis.Close(false)
	dis.RegisterHandler(cb1, false, postfix)

	cb2 := func(pb *InnerMessage, reply string, err string) {
		fmt.Println("2 pb", pb, "reply", reply, "err", err)
	}
	dis1 := createTestDispatcher()
	defer dis1.Close(false)
	dis1.RegisterHandler(cb2, false, postfix)

	dis3 := createTestDispatcher()
	defer dis3.Close(false)

	// subject:'test.*InnerMessage.fdasfdas'
	dis3.Notify(&InnerMessage{
		Host: proto.String("127.0.0.1"),
	}, postfix)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for {
			m := <-dis.MsgChan()
			dis.Process(m)
			wg.Done()
		}
	}()
	go func() {
		for {
			m2 := <-dis1.MsgChan()
			dis1.Process(m2)
			wg.Done()
		}
	}()
	wg.Wait()
}

func TestNatsDispatcher_GroupNotify(t *testing.T) {
	wg := sync.WaitGroup{}

	postfix := "fdasfdas"

	cb1 := func(pb *InnerMessage, reply string, err string) {
		fmt.Println("1 pb", pb, "reply", reply, "err", err)
		wg.Done()
	}
	dis := createTestDispatcher()
	defer dis.Close(false)
	dis.RegisterHandler(cb1, true, postfix)
	go func() {
		for {
			m := <-dis.MsgChan()
			dis.Process(m)
		}

	}()
	cb2 := func(pb *InnerMessage, reply string, err string) {
		fmt.Println("2 pb", pb, "reply", reply, "err", err)
		wg.Done()
	}
	dis1 := createTestDispatcher()
	defer dis1.Close(false)
	dis1.RegisterHandler(cb2, true, postfix)
	go func() {
		for {
			m := <-dis1.MsgChan()
			dis1.Process(m)
		}
	}()

	wg.Add(1)
	dis.Notify(&InnerMessage{
		Host: proto.String("127.0.0.1"),
	}, postfix)

	wg.Wait()
}

func TestNatsDispatcher_Request(t *testing.T) {
	dis := createTestDispatcher()
	defer dis.Close(false)

	dis1 := createTestDispatcher()
	defer dis1.Close(false)

	postfix := "fdasfdas"

	h := func(pb *InnerMessage, reply string, err string) {
		fmt.Println("req pb", pb, "reply", reply, "err", err)
		dis.Replay(reply, &OtherMessage{Key: proto.Int64(1002)})
	}
	dis.RegisterHandler(h, true, postfix)
	go func() {
		for {
			m := <-dis.MsgChan()
			dis.Process(m)
		}
	}()

	wg := sync.WaitGroup{}
	cb := func(pb *OtherMessage, reply string, err string) {
		fmt.Println("resp pb=", pb, "reply=", reply, "err=", err)
		wg.Done()
	}
	wg.Add(1)
	dis1.Request(&InnerMessage{
		Host: proto.String("127.0.0.2"),
	}, cb, postfix)
	go func() {
		for {
			m := <-dis1.MsgChan()
			dis1.Process(m)
		}
	}()

	wg.Wait()
}

func TestNatsDispatcher_RegisterHandler(t *testing.T) {
	dis := createTestDispatcher()
	defer dis.Close(false)
	dis1 := createTestDispatcher()
	defer dis1.Close(false)
	wg := sync.WaitGroup{}
	dis.RegisterHandler(func(pb *InnerMessage, reply string, error string) {
		fmt.Println("dis", error)
		wg.Done()
	}, false)
	dis1.RegisterHandler(func(pb *InnerMessage, reply string, error string) {
		fmt.Println("dis1", error)
		wg.Done()
	}, false)
	for i := 0; i < 10; i++ {
		dis.Notify(&InnerMessage{
			Host: proto.String("127.0.0.1"),
		})
		wg.Add(2)
	}

	go func() {
		for m := range dis.MsgChan() {
			dis.Process(m)
		}
	}()
	go func() {
		for m := range dis1.MsgChan() {
			dis1.Process(m)
		}
	}()
	wg.Wait()
}

func TestNatsDispatcher_RegisterSyncHandler(t *testing.T) {
	dis := createTestDispatcher()
	defer dis.Close(false)
	wg := sync.WaitGroup{}
	dis.RegisterSyncHandler(func(pb *InnerMessage, reply string, error string) {
		fmt.Println("dis", error)
		wg.Done()
	}, false)

	for i := 0; i < 10; i++ {
		dis.Notify(&InnerMessage{
			Host: proto.String("127.0.0.1"),
		})
		wg.Add(1)
	}

	go func() {
		for m := range dis.MsgChan() {
			dis.Process(m)
		}
	}()
	wg.Wait()
}

func TestNatsDispatcher_RegisterGroupedSyncHandler(t *testing.T) {
	dis := createTestDispatcher()
	defer dis.Close(false)

	wg := sync.WaitGroup{}
	dis.RegisterSyncHandler(func(pb *InnerMessage, reply string, error string) {
		fmt.Println("dis", error)
		wg.Done()
	}, true)
	go func() {
		for m := range dis.MsgChan() {
			dis.Process(m)
		}
	}()

	dis1 := createTestDispatcher()
	defer dis1.Close(false)
	dis1.RegisterSyncHandler(func(pb *InnerMessage, reply string, error string) {
		fmt.Println("dis1", error)
		wg.Done()
	}, true)
	go func() {
		for m := range dis1.MsgChan() {
			dis1.Process(m)
		}
	}()

	for i := 0; i < 10; i++ {
		dis.Notify(&InnerMessage{
			Host: proto.String("127.0.0.1"),
		})
		wg.Add(1)
	}

	wg.Wait()
}

func BenchmarkDispatcher_Notify(b *testing.B) {

	dis := createTestDispatcher()
	defer dis.Close(false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dis.Notify(&InnerMessage{
			Host: proto.String("127.0.0.1"),
		}, 1000410001)
	}
}

func BenchmarkDispatcher_Request(b *testing.B) {

	dis := createTestDispatcher()
	defer dis.Close(false)
	h := func(pb *InnerMessage, reply string, err string) {
		//fmt.Println("req pb", pb, "reply", reply, "err", err)
		dis.Replay(reply, &OtherMessage{Key: proto.Int64(1002)})
	}
	dis.RegisterHandler(h, true)
	go func() {
		for m := range dis.MsgChan() {
			dis.Process(m)
		}
	}()

	dis2 := createTestDispatcher()
	defer dis2.Close(false)
	cb := func(pb *OtherMessage, reply string, err string) {
		//fmt.Println("resp pb=", pb, "reply=", reply, "err=", err)
	}
	go func() {
		for m := range dis2.MsgChan() {
			dis2.Process(m)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dis.Request(&InnerMessage{
			Host: proto.String("127.0.0.1"),
		}, cb)
	}
}

func BenchmarkDispatcher_Process(b *testing.B) {
	dis := createTestDispatcher()

	cb := func(pb *OtherMessage, reply string, err string) {
	}
	m := &msg{
		handler: reflect.ValueOf(cb),
		arg:     reflect.ValueOf(&OtherMessage{}),
		reply:   valEmptyString,
		err:     valEmptyString,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dis.Process(m)
	}
}
