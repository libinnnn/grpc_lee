package codec

import "io"

type Header struct {
	ServiceMethod string // format "service.method"
	Seq	uint64 // 客户端生成的序列号，用于标识请求
	Error string
}

// 消息编解码器
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(conn io.ReadWriteCloser) Codec
type Type string

const (
	GobType Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init()  {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}

