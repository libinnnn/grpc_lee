package server

import (
	"encoding/json"
	"errors"
	"grpc_lee/codec"
	"grpc_lee/service"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
)

const MagicNumber = 0x3bef5c

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *service.MethodType
	svc          *service.Service
}

type Option struct {
	MagicNumber int        // 用于标记grpc-lee调用
	CodecType   codec.Type // 选择方式对数据进行编码
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// rpc server
type Server struct {
	serviceMap sync.Map
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

	s.serverCodec(f(conn))
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

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	err := req.svc.Call(req.mtype, req.argv, req.replyv)
	if err != nil {
		req.h.Error = err.Error()
		s.sendResponse(cc, req.h, invalidRequest, sending)
		return
	}
	s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

var invalidRequest = struct{}{}

func (s *Server) serverCodec(cc codec.Codec) {
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
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg)
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
