package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"grpc_lee/codec"
	"grpc_lee/service"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	Connected        = "200 Connected to GRPC_lee"
	DefaultRPCPath   = "/_gprc_lee_"
	DefaultDebugPath = "/debug/grpc_lee"
	MagicNumber      = 0x3bef5c
)

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *service.MethodType
	svc          *service.Service
}

type Option struct {
	MagicNumber   int           // 用于标记grpc-lee调用
	CodecType     codec.Type    // 选择方式对数据进行编码
	Conncttimeout time.Duration // 连接超时
	HandleTimeout time.Duration // 处理超时
}

var DefaultOption = &Option{
	MagicNumber:   MagicNumber,
	CodecType:     codec.GobType,
	Conncttimeout: time.Second * 10,
}

// rpc server
type Server struct {
	serviceMap sync.Map
}

type Handler interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func (s *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		// 接收到请求之后，开启协程处理任务
		go s.ServerConn(conn)

	}
}

// 最佳实践：对外暴露的函数都通过一个默认实例对象来提供，方便后期对处理函数进行扩展替换
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func (s *Server) ServerConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	// 这里的报文头统一采用json的格式进行魔数还有报文解析格式的协商
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType] // 从map中选取对应的解码器
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	s.serverCodec(f(conn), opt.HandleTimeout)
}

func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}

	return &h, nil
}

// 解析请求，通过反射机制构造对应的调用函数和参数
func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{
		h: h,
	}
	req.svc, req.mtype, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.NewArgv()
	req.replyv = req.mtype.NewReplyv()

	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.Call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

var invalidRequest = struct{}{}

func (s *Server) serverCodec(cc codec.Codec, timeout time.Duration) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		// 解析请求
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)                                         // 等待
		go s.handleRequest(cc, req, sending, wg, timeout) // 调用协程进行处理，提高处理效率
	}
	wg.Wait() // 等待异步处理完成之后关闭连接
	_ = cc.Close()
}

func (s *Server) Register(rcvr interface{}) error {
	service := service.NewService(rcvr)
	if _, dup := s.serviceMap.LoadOrStore(service.Name, service); dup {
		return errors.New("rpc: service already defined: " + service.Name)
	}
	return nil
}

// 对外提供一个默认的注册器进行注册
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

func (s *Server) findService(serviceMethod string) (svc *service.Service, mtype *service.MethodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc erver: service/method request ")
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can not find service: " + serviceName)
		return
	}
	svc = svci.(*service.Service)
	mtype = svc.Method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can not find method " + methodName)
	}
	return
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+Connected+"\n\n")
	s.ServerConn(conn)
}

func (s *Server) HandleHTTP() {
	http.Handle(DefaultRPCPath, s)
	http.Handle(DefaultDebugPath, debugHTTP{s})
	log.Println("rpc server debug path: ", DefaultDebugPath)
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
