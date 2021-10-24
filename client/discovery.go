package client

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect SelectMode = iota
	RoundRobinSelect
)

type Discovery interface {
	Refresh() error // 从远程服务器上更新实例
	Update(servers []string) error
	Get(mode SelectMode) (string, error)
	GetAll() ([]string, error)
}

type MultiServersDiscovery struct {
	r       *rand.Rand
	Mu      sync.RWMutex
	Servers []string
	index   int
}

func (m MultiServersDiscovery) Refresh() error {
	return nil
}

func (m MultiServersDiscovery) Update(servers []string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.Servers = servers
	return nil
}

// 根据mode负载均衡策略决定返回对应的client
func (m MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	n := len(m.Servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		return m.Servers[m.r.Intn(n)], nil
	case RoundRobinSelect:
		s := m.Servers[m.index%n] // 下标会被更新，因此通过取模的方式能保证访问安全
		m.index = (m.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

func (m MultiServersDiscovery) GetAll() ([]string, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	servers := make([]string, len(m.Servers), len(m.Servers))
	copy(servers, m.Servers)
	return servers, nil
}

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
		Servers: servers,
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}

var _ Discovery = (*MultiServersDiscovery)(nil)
