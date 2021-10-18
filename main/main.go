package main

import (
	"context"
	"encoding/json"
	"fmt"
	"grpc_lee/client"
	"grpc_lee/codec"
	"grpc_lee/server"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Foo int
type Args struct {
	Num1, Num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
	*reply = args.Num1 + args.Num2
	return nil
}

func startHTTPServer(addr chan string) {
	// 注册函数
	var foo Foo
	if err := server.Register(&foo); err != nil {
		log.Fatal("register err: ", err)
	}
	// 开启端口
	l, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	server.HandleHTTP()
	addr <- l.Addr().String()
	_ = http.Serve(l, nil)
}

func startTCPServer(addr chan string) {
	// 注册函数
	var foo Foo
	if err := server.Register(&foo); err != nil {
		log.Fatal("register err: ", err)
	}
	// 开启端口
	l, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	server.Accept(l)
}

func startServer(addrCh chan string) {
	var foo Foo
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	_ = server.Register(&foo)
	addrCh <- l.Addr().String()
	server.Accept(l)
}

func call(addr chan string) {
	rpcClient, err := client.DialHTTP("tcp", <-addr)
	if err != nil {
		log.Fatalf("rpc client dial http fail: %s", err.Error())
	}
	defer func() { _ = rpcClient.Close() }()

	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{
				Num1: i,
				Num2: i * i,
			}
			var reply int
			if err := rpcClient.Call(context.Background(), "Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error: ", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}

func foo(xc *client.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcast":
		err = xc.BroadCast(ctx, serviceMethod, args, &reply)
	}

	if err != nil {
		log.Printf("%s %s error: %v", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num1, args.Num2, reply)
	}
}

func balanceCall(addr1, addr2 string) {
	d := client.NewMultiServerDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	xc := client.NewXClient(d, client.RandomSelect, nil)
	defer func() { _ = xc.Close() }()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func broadcast(addr1, addr2 string) {
	d := client.NewMultiServerDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	xc := client.NewXClient(d, client.RandomSelect, nil)
	defer func() { _ = xc.Close() }()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{Num1: i, Num2: i * i})
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(xc, ctx, "broadcast", "Foo.Sleep", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func main() {
	//// part1 - http server 调用
	//log.SetFlags(0)
	//addr := make(chan string)
	//go call(addr)
	//startHTTPServer(addr) // 由于会阻塞进程，因此需要放在后面

	// part2 - load balance call
	log.SetFlags(0)
	ch1 := make(chan string)
	ch2 := make(chan string)
	go startServer(ch1)
	go startServer(ch2)
	addr1 := <-ch1
	addr2 := <-ch2

	time.Sleep(time.Second * 3)
	balanceCall(addr1, addr2)
	broadcast(addr1, addr2)
}

func easyCall() {
	addr := make(chan string)
	go startTCPServer(addr)

	// in fact, following code is like a simple geerpc client
	conn, _ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)
	// send options
	_ = json.NewEncoder(conn).Encode(server.DefaultOption)
	cc := codec.NewGobCodec(conn)
	// send request & receive response
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		_ = cc.Write(h, fmt.Sprintf("grpc-lee req %d", h.Seq))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
