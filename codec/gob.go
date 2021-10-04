package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser // 通过TCP或者unix建立socket时得到的链接实例
	buf  *bufio.Writer      // 为了防止阻塞而创建带缓冲的buf
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func (g GobCodec) Close() error {
	return g.conn.Close()
}

func (g GobCodec) ReadHeader(h *Header) error {
	return g.dec.Decode(h)
}

func (g GobCodec) ReadBody(body interface{}) error {
	return g.dec.Decode(body)
}

func (g GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = g.buf.Flush()
		if err != nil {
			_ = g.Close()
		}
	}()

	if err := g.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := g.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}
