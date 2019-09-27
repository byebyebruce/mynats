package mynats

import (
	"errors"
	"reflect"
	"time"

	"github.com/alecthomas/log4go"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/encoders/protobuf"
)

const (
	callbackParameter = 3                              // 回掉函数参数个数
	timeOut           = time.Second * time.Duration(3) // 超时
)

var (
	errorFuncType  = errors.New("handler must be a function like [func(pb *proto.MyUser, reply string, err string)]")
	valEmptyString = reflect.ValueOf("")
)

// Handler 消息处理函数
// 格式：func(pb *proto.MyUser, reply string, err string)
type Handler interface{}

// msg 异步回掉消息，用reflect.Value主要为了不在主线程掉reflect.ValueOf
type msg struct {
	handler reflect.Value // 回掉函数的value
	arg     reflect.Value // 参数的value
	reply   reflect.Value // 回复字符串的value
	err     reflect.Value // 错误字符串的value
}

// Dispatcher NATS消息分发器
type Dispatcher struct {
	enc       *nats.EncodedConn  // NATS的Conn
	cluster   string             // 集群名字
	msgChan   chan *msg          // 消息通道
	argArray  []reflect.Value    // value参数数组，防止多次分配数组
	handleMap map[string]Handler // 消息处理函数map
}

// NewDispatcher 构造器
func NewDispatcher(cfg *Config, name string, maxMsg int) (*Dispatcher, error) {

	// 设置参数
	opts := make([]nats.Option, 0)
	opts = append(opts, nats.Name(name))
	if len(cfg.User) > 0 {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Pwd))
	}
	opts = append(opts, nats.ReconnectWait(time.Second*time.Duration(cfg.ReconnectWait)))
	opts = append(opts, nats.MaxReconnects(int(cfg.MaxReconnects)))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log4go.Warn("[Dispatcher] nats.Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.DiscoveredServersHandler(func(nc *nats.Conn) {
		log4go.Info("[Dispatcher] nats.DiscoveredServersHandler", nc.DiscoveredServers())
	}))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log4go.Warn("[Dispatcher] nats.Disconnect")
	}))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		if nil == err {
			log4go.Info("[Dispatcher] nats.DisconnectErrHandler")
		} else {
			log4go.Warn("[Dispatcher] nats.DisconnectErrHandler,error=[%v]", err)
		}
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log4go.Warn("[Dispatcher] nats.ClosedHandler")
	}))
	opts = append(opts, nats.ErrorHandler(func(nc *nats.Conn, subs *nats.Subscription, err error) {
		log4go.Warn("[Dispatcher] nats.ErrorHandler subs=[%s] error=[%s]", subs.Subject, err.Error())
	}))

	// 创建nats client
	nc, err := nats.Connect(cfg.Server, opts...)
	if err != nil {
		return nil, err
	}
	enc, err1 := nats.NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER)
	if nil != err1 {
		return nil, err1
	}

	d := &Dispatcher{
		enc:       enc,
		cluster:   cfg.Cluster,
		handleMap: make(map[string]Handler),
		msgChan:   make(chan *msg, maxMsg),
		argArray:  make([]reflect.Value, callbackParameter),
	}
	return d, nil
}

// RawConn 返回conn
func (dispatcher *Dispatcher) RawConn() *nats.EncodedConn {
	return dispatcher.enc
}

// Notify 推送(不需要回复)
// cb格式:func(pb *proto.MyUser ,reply string, err string)
// subjectPostfix是subject的后缀 例如：m是*pb.MyUser类型的对象，subjectPostfix是1000410001，那subject是 "*pb.MyUser.1000410001"
func (dispatcher *Dispatcher) Notify(m proto.Message, subjectPostfix ...interface{}) {
	argVal := reflect.TypeOf(m)
	sub := joinSubject(dispatcher.cluster, argVal.String(), subjectPostfix...)
	if err := dispatcher.enc.Publish(sub, m); nil != err {
		log4go.Error("[Dispatcher] Notify sub=[%s] error=[%s]", sub, err.Error())
	} else {
		log4go.Debug("[Dispatcher] Notify sub=[%s]", sub)
	}
}

// Request 请求
// cb格式:func(pb *proto.MyUser ,reply string, err string)
// subjectPostfix是subject的后缀 例如：m是*pb.MyUser类型的对象，subjectPostfix是1000410001，那subject是 "*pb.MyUser.1000410001"
func (dispatcher *Dispatcher) Request(m proto.Message, cb interface{}, subjectPostfix ...interface{}) {
	argVal := reflect.TypeOf(m)
	sub := joinSubject(dispatcher.cluster, argVal.String(), subjectPostfix...)
	cbValue := reflect.ValueOf(cb)
	cbType := reflect.TypeOf(cb)

	// TODO 运行时linux下不检查cb，其他的平台要检查cb
	go func() {
		argType := cbType.In(0)
		oPtr := reflect.New(argType.Elem())
		err := dispatcher.enc.Request(sub, m, oPtr.Interface(), timeOut)
		errVal := valEmptyString
		if nil != err {
			errVal = reflect.ValueOf(err.Error())
		}
		dispatcher.msgChan <- &msg{
			handler: cbValue,
			arg:     oPtr,
			reply:   valEmptyString,
			err:     errVal,
		}
		if nil != err {
			log4go.Error("[Dispatcher] Request sub=[%s] error=[%s]", sub, err.Error())
		} else {
			log4go.Debug("[Dispatcher] Request over sub=[%s]", sub)
		}
	}()
	log4go.Debug("[Dispatcher] Request sub=[%s]", sub)
}

// RequestSync 同步请求
// cb格式:func(pb *proto.MyUser ,reply string, err string)
// subjectPostfix是subject的后缀 例如：m是*pb.MyUser类型的对象，subjectPostfix是1000410001，那subject是 "*pb.MyUser.1000410001"
func (dispatcher *Dispatcher) RequestSync(req proto.Message, resp proto.Message, subjectPostfix ...interface{}) error {
	argVal := reflect.TypeOf(req)
	sub := joinSubject(dispatcher.cluster, argVal.String(), subjectPostfix...)
	return dispatcher.enc.Request(sub, req, resp, timeOut)
}

// Replay 回复消息
func (dispatcher *Dispatcher) Replay(reply string, m proto.Message) {
	if err := dispatcher.enc.Publish(reply, m); nil != err {
		log4go.Error("[Dispatcher] Replay reply=[%s] error=[%s]", reply, err.Error())
	} else {
		log4go.Debug("[Dispatcher] Replay reply=[%s]", reply)
	}
}

func checkHandler(cb interface{}) (reflect.Type, error) {
	cbType := reflect.TypeOf(cb)
	if cbType.Kind() != reflect.Func {
		return nil, errorFuncType
	}

	numArgs := cbType.NumIn()
	if callbackParameter != numArgs {
		return nil, errorFuncType
	}

	argType := cbType.In(0)
	if argType.Kind() != reflect.Ptr {
		return nil, errorFuncType
	}
	if cbType.In(1).Kind() != reflect.String {
		return nil, errorFuncType
	}
	if cbType.In(2).Kind() != reflect.String {
		return nil, errorFuncType
	}
	oPtr := reflect.New(argType.Elem())
	_, ok := oPtr.Interface().(proto.Message)
	if !ok {
		return nil, errorFuncType
	}
	return argType, nil
}

func (dispatcher *Dispatcher) parseHandler(cb interface{}, subjectPostfix ...interface{}) (func(*nats.Msg), string) {
	// 检查回掉函数的格式
	argType, err := checkHandler(cb)
	if nil != err {
		panic(err)
	}
	// 算subject
	sub := joinSubject(dispatcher.cluster, argType.String(), subjectPostfix...)

	cbValue := reflect.ValueOf(cb)

	// 回掉函数
	h := func(m *nats.Msg) {
		argVal := reflect.New(argType.Elem())
		pb := argVal.Interface().(proto.Message)
		if err := proto.Unmarshal(m.Data, pb); nil != err {
			log4go.Error("[Dispatcher] cb proto.Unmarshal error=[%s]", err.Error())
		} else {
			dispatcher.msgChan <- &msg{
				handler: cbValue,
				arg:     argVal,
				reply:   reflect.ValueOf(m.Reply),
				err:     valEmptyString,
			}
		}
		log4go.Debug("[Dispatcher] callback sub=[%s] reply=[%s]", m.Subject, m.Reply)
	}
	return h, sub
}

// RegisterHandler 注册异步回掉函数，消息会发送消息通道msgChan
// grouped:是否分组(分组的只有一个能收到)
// cb格式:func(pb *proto.MyUser ,reply string, err string)
// subjectPostfix是subject的后缀 例如：m是*pb.MyUser类型的对象，subjectPostfix是1000410001，那subject是 "*pb.MyUser.1000410001"
func (dispatcher *Dispatcher) RegisterHandler(cb interface{}, grouped bool, subjectPostfix ...interface{}) {
	h, sub := dispatcher.parseHandler(cb, subjectPostfix...)
	if _, ok := dispatcher.handleMap[sub]; ok {
		panic("handler exists")
	}
	// 保存下，防止重复
	dispatcher.handleMap[sub] = cb

	var err error
	if grouped {
		_, err = dispatcher.enc.QueueSubscribe(sub, dispatcher.cluster, h)
	} else {
		_, err = dispatcher.enc.Subscribe(sub, h)
	}
	if nil != err {
		panic(err)
	}
	log4go.Debug("[Dispatcher] RegisterHandler=[%s] grouped[%v]", sub, grouped)
}

func (dispatcher *Dispatcher) parseSyncHandler(cb interface{}, subjectPostfix ...interface{}) (func(*nats.Msg), string) {
	// 检查回掉函数的格式
	argType, err := checkHandler(cb)
	if nil != err {
		panic(err)
	}

	cbValue := reflect.ValueOf(cb)

	sub := joinSubject(dispatcher.cluster, argType.String(), subjectPostfix...)
	h := func(m *nats.Msg) {
		argVal := reflect.New(argType.Elem())
		pb := argVal.Interface().(proto.Message)
		if err := proto.Unmarshal(m.Data, pb); nil != err {
			log4go.Error("[Dispatcher] cb proto.Unmarshal error=[%s]", err.Error())
		} else {
			cbValue.Call([]reflect.Value{argVal, reflect.ValueOf(m.Reply), valEmptyString})
		}
		log4go.Debug("[Dispatcher] sync callback sub=[%s] reply=[%s]", m.Subject, m.Reply)
	}
	return h, sub
}

// RegisterSyncHandler 注册同步回掉函数
// grouped:是否分组(分组的只有一个能收到)
// cb格式:func(pb *proto.MyUser ,reply string, err string)
// subjectPostfix是subject的后缀 例如：m是*pb.MyUser类型的对象，subjectPostfix是1000410001，那subject是 "*pb.MyUser.1000410001"
func (dispatcher *Dispatcher) RegisterSyncHandler(cb interface{}, grouped bool, subjectPostfix ...interface{}) {
	h, sub := dispatcher.parseSyncHandler(cb, subjectPostfix...)

	var err error
	if grouped {
		_, err = dispatcher.enc.QueueSubscribe(sub, dispatcher.cluster, h)
	} else {
		_, err = dispatcher.enc.Subscribe(sub, h)
	}
	if nil != err {
		panic(err)
	}

	log4go.Debug("[Dispatcher] RegisterSyncHandler=[%s] grouped[%v]", sub, grouped)
}

// MsgChan 消息通道
func (dispatcher *Dispatcher) MsgChan() <-chan *msg {
	return dispatcher.msgChan
}

// Process 处理消息
func (dispatcher *Dispatcher) Process(m *msg) {
	dispatcher.argArray[0] = m.arg
	dispatcher.argArray[1] = m.reply
	dispatcher.argArray[2] = m.err
	m.handler.Call(dispatcher.argArray)
}

// Close 关闭
// 是否需要处理完通道里的消息
func (dispatcher *Dispatcher) Close(process bool) {
	if process {
		for exit := false; exit; {
			select {
			case m := <-dispatcher.msgChan:
				dispatcher.Process(m)
			default:
				exit = true
			}
		}
	}
	dispatcher.enc.FlushTimeout(time.Duration(3 * time.Second))
	dispatcher.enc.Close()
}
