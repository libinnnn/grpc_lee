package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"grpc_lee/codec"
	"grpc_lee/server"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// 在编译期间确定该对象一定实现了某个接口的方法，也方ide自动生成
var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

type newClientFunc func(conn net.Conn, opt *server.Option) (client *Client, err error)

type Call struct {
	Seq           uint64
	ServiceMethod string      // format "service.method"
	Args          interface{} // 调用参数
	Reply         interface{} // 返回值
	Error         error
	Done          chan *Call // 调用完成时提示,支持异步调用
}

func (call *Call) done() {
	call.Done <- call
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
	return !c.closing && !c.shutdown // 初始状态都为false
}

type clientResult struct {
	client *Client
	err    error
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
	for err == nil { // 由于socket read 操作是阻塞式的，因此当读不到数据的时候gouroutine会被挂起阻塞
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
			call.done() // 只有调用call.done才标志着调用结束，因此当write之后，会等待receive协程进行处理
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
		call := c.removeCall(seq) // 当出现写入错误的时候，应该将该调用移除
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
func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := c.Go(serviceMethod, args, reply, make(chan *Call, 1)) // 这里缓冲区设置成1，因此是阻塞式的
	select {
	case <-ctx.Done(): // 可以接收客户端的信号决定什么时候停止
		c.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
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

func NewHTTPClient(conn net.Conn, opt *server.Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", server.DefaultRPCPath))

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == server.Connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}

	return nil, err
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
	return dialTimeout(NewClient, network, address, opts...)
}

func DialHTTP(network, address string, opts ...*server.Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

func dialTimeout(f newClientFunc, network, address string, opts ...*server.Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.Conncttimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult) // 需要将返回值作为管道变量一起传递过去
	go func() {
		client, err := f(conn, opt)
		if err != nil {
			log.Fatalf("rpc client initial fail: %s", err.Error())
		}
		ch <- clientResult{
			client: client,
			err:    err,
		}
	}()
	if opt.Conncttimeout == 0 { // 超时时间为0代表着无限制
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.Conncttimeout): // 如果在超时时间内没有返回，则返回错误
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.Conncttimeout)
	case result := <-ch:
		return result.client, result.err
	}

}

func Xdail(rpcAddr string, opts ...*server.Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocal, addr := parts[0], parts[1]
	switch protocal {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		return Dail(protocal, addr, opts...)
	}
}
