package codec

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber  = 0x3bef5c

type request struct {
	h *Header
	argv, replyv reflect.Value
}

type Option struct {
	MagicNumber int // 用于标记grpc-lee调用
	CodecType Type // 选择方式对数据进行编码
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType: GobType,
}

// rpc server
type Server struct {}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer  = NewServer()

func (s *Server) Accept(lis net.Listener)  {
	for{
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go s.ServerConn(conn)

	}
}

func Accept(lis net.Listener)  {
	DefaultServer.Accept(lis)
}

func (s *Server) ServerConn(conn io.ReadWriteCloser){
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil{
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	s.serverCodec(f(conn))
}

func (s *Server) readRequestHeader(cc Codec) (*Header, error){
	var h Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}

	return &h, nil
}

func (s *Server) readRequest(cc Codec) (*request, error){
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return  nil, err
	}
	req := &request{
		h:      h,
	}
	// Todo: 请求参数待处理

	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

func (s *Server) sendResponse(cc Codec, h *Header, body interface{}, sending *sync.Mutex)  {
	sending.Lock()
	defer  sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	// TODO: 调用RPC服务
	defer wg.Done()
	log.Println("正在处理......")
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("grpc-lee resp %d", req.h.Seq))
	log.Println(req.replyv)
	s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

var invalidRequest = struct {}{}

func(s *Server) serverCodec(cc Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for{
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
	wg.Wait()
	_ = cc.Close()
}
