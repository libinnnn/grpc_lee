package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// 注册中心，用于注册中心与服务之间的交互
type Registry struct {
	timeout time.Duration
	mu      sync.Mutex             // 操作互斥锁
	servers map[string]*ServerItem // 注册中心用于存放可用节点节点
}

// 当前节点
type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_grpc_lee_/registry"
	defaultTimeout = time.Minute * 5
)

func New(timeout time.Duration) *Registry {
	return &Registry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var defaultRegister = New(defaultTimeout)

// 向注册中心存放节点
func (r *Registry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	// 更新逻辑为当前节点不存在则插入，存在则更新start time
	if s == nil {
		r.servers[addr] = &ServerItem{
			Addr:  addr,
			start: time.Now(),
		}
	} else {
		s.start = time.Now()
	}
}

// 判断并重新构建当前可以的服务实例
func (r *Registry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// 服务于 /_grpc_lee_/registry，用于注册中心处理心跳信号
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET": // 用于向注册中心查询当前存活的节点
		w.Header().Set("X-Grpc-Server", strings.Join(r.aliveServers(), ","))
	case "POST": // 向注册中心进行地址注册
		addr := req.Header.Get("X-Grpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Registry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	defaultRegister.HandleHTTP(defaultPath)
}

func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	// 协程for循环运行，每隔一点时间时间，发送心跳信号
	go func() {
		t := time.NewTicker(duration) // 周期性定时器，每隔一个时间段内发送发送当前的系统时间
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Grpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
