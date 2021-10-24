package registry

import (
	"grpc_lee/client"
	"log"
	"net/http"
	"strings"
	"time"
)

// 用于客户端向注册中心进行服务的发现
type RegistryDiscovery struct {
	*client.MultiServersDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
}

const defaultUpdateTimeout = time.Second * 10

func NewRegistryDiscovery(registerAddr string, timeout time.Duration) *RegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &RegistryDiscovery{
		MultiServersDiscovery: client.NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

func (d *RegistryDiscovery) Update(servers []string) error {
	d.Mu.Lock()
	defer d.Mu.Lock()
	d.Servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *RegistryDiscovery) Refresh() error {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Grpc-Server"), ",")
	d.Servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.Servers = append(d.Servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *RegistryDiscovery) Get(mode client.SelectMode) (string, error) {
	// 调用refresh确保服务列表没有过期
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *RegistryDiscovery) GetAll() ([]string, error) {
	// 调用refresh确保服务列表没有过期
	if err := d.Refresh(); err != nil {
		return nil, err
	}

	return d.MultiServersDiscovery.GetAll()
}
