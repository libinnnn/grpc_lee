package client

import (
	"fmt"
	"grpc_lee/server"
	"net"
	"os"
	"runtime"
	"testing"
)

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func TestXDail(t *testing.T) {
	if runtime.GOOS == "linux" {
		ch := make(chan struct{})
		addr := "/tmp/grpc_lee.sock"
		go func() {
			_ = os.Remove(addr)
			l, err := net.Listen("unix", addr)
			if err != nil {
				t.Fatal("failed to listen unix socket")
			}
			ch <- struct{}{}
			server.Accept(l)
		}()
		<-ch
		_, err := Xdail("unix@" + addr)
		_assert(err == nil, "failed to connect unix socket")
	}
}
