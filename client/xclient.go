package client

import (
	"context"
	"grpc_lee/server"
	"io"
	"reflect"
	"sync"
)

type XClient struct {
	d       Discovery
	mode    SelectMode
	opt     *server.Option
	mu      sync.Mutex
	clients map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

func (x *XClient) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	for key, client := range x.clients {
		_ = client.Close()
		delete(x.clients, key)
	}
	return nil
}

func (x *XClient) dail(rpcAddr string) (*Client, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	client, ok := x.clients[rpcAddr]
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(x.clients, rpcAddr)
		client = nil
	}
	if client == nil { // 如果缓存中存在，则直接复用
		var err error
		client, err := Xdail(rpcAddr, x.opt)
		if err != nil {
			return nil, err
		}
		x.clients[rpcAddr] = client
	}
	return client, nil
}

func (x *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := x.dail(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

func (x *XClient) Call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := x.d.Get(x.mode)
	if err != nil {
		return err
	}
	return x.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// 广播所有服务实例
// 任意实例发生错误，返回其中一个错误
// 调用成功，返回其中一个结果
func (x *XClient) BroadCast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := x.d.GetAll()
	if err != nil {
		return nil
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface() // 通过反射创建一个对象拷贝
			}
			err := x.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel() // 出现了错误之后，即终止调用
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}

func NewXClient(d Discovery, mode SelectMode, opt *server.Option) *XClient {
	return &XClient{
		d:    d,
		mode: mode,
		opt:  opt,
	}
}
