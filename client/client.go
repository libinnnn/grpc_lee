package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"grpc_lee/codec"
	"grpc_lee/server"
	"io"
	"log"
	"net"
	"sync"
)

// 在编译期间确定该对象一定实现了某个接口的方法，也方ide自动生成
var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

type Call struct {
	Seq           uint64
	ServiceMethod string      // format "service.method"
	Args          interface{} // 调用参数
	Reply         interface{} // 返回值
	Error         error
	Done          chan *Call // 调用完成时提示,支持异步调用
}

func (c *Call) done() {
	c.Done <- c
}

type Client struct {
	cc       codec.Codec
	opt      *server.Option
	sending  sync.Mutex // 保证请求有序发送
	header   codec.Header
	mu       sync.Mutex
	seq      uint64
	pending  map[uint64]*Call
	closing  bool //  用户是否已经调用结束，是用户主动关闭
	shutdown bool // 服务端告诉我们是否结束，由错误发生时
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrShutdown
	}
	c.closing = true
	return c.cc.Close()
}

func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closing && !c.shutdown
}

// 注册调用，并且客户端的调用序号保证自增
func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			break
		}
		call := c.removeCall(h.Seq)
		switch {
		case call == nil: // 请求没有发送完整或者被中断了，但是服务端仍然处理了
			err = c.cc.ReadBody(nil)
		case h.Error != "": // 服务端处理出错
			call.Error = fmt.Errorf(h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		default: // 正常响应
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// error出现了，需要通知所有的调用错误信息
	c.terminateCalls(err)
}

func (c *Client) send(call *Call) {
	// 保证发送包的完整性
	c.sending.Lock()
	defer c.sending.Unlock()

	// 客户端注册调用
	seq, err := c.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 创建请求头
	c.header.ServiceMethod = call.ServiceMethod
	c.header.Seq = seq
	c.header.Error = ""

	// 编码并发送请求
	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}

}

// 异步接口
func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10) // 缓冲区，防止阻塞
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel buffer is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	return call
}

// 同步接口
func (c *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-c.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done // 这里缓冲区设置成1，因此是阻塞式的
	return call.Error
}

func NewClient(conn net.Conn, opt *server.Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error: ", err)
		return nil, err
	}
	// send option with server
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options err: ", err)
		_ = conn.Close()
	}

	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *server.Option) *Client {
	client := &Client{
		cc:      cc,
		opt:     opt,
		seq:     1,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

// 简化用户调用，将option实现为可选参数
func parseOptions(opts ...*server.Option) (*server.Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return server.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of option is more then 1")
	}
	opt := opts[0]
	opt.MagicNumber = server.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = server.DefaultOption.CodecType
	}
	return opt, nil
}

func Dail(network, address string, opts ...*server.Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
}
