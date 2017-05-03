package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// 生产者和Registration关系的维护
// 同一个Registration对应多个生存者
type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]Producers
}

type Registration struct {
	Category string
	Key      string
	SubKey   string
}
type Registrations []Registration

// 端点信息存储
type PeerInfo struct {
	lastUpdate       int64 // 最近一次更新
	id               string
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// 生产者定义
type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool      // 是否要被移除
	tombstonedAt time.Time // 移除时间
}

type Producers []*Producer

// 格式化输出信息
func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

// 将Producer标记为墓碑状态（需要被删除）
func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

// 被标记为墓碑状态,同时距标记时间小于lifetime值。
func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

/*********************************************NewRegistrationDB************************************************************/

// 创建RegistrationDB对象
func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]Producers),
	}
}

// add a registration key
// 添加一个registration的key
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = Producers{}
	}
}

// add a producer to a registration
// 将一个Producer添加到指定的Registration里
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	producers := r.registrationMap[k]
	found := false
	for _, producer := range producers {
		// 判断生存者是否已经被添加
		if producer.peerInfo.id == p.peerInfo.id {
			found = true
		}
	}
	if found == false {
		// 如果Producer没有被添加过那么就添加进去
		r.registrationMap[k] = append(producers, p)
	}
	return !found
}

// remove a producer from a registration
// 从registration里删除一个Producer
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		// 本身不存在，即返回
		return false, 0
	}
	removed := false
	cleaned := Producers{}
	for _, producer := range producers {
		if producer.peerInfo.id != id {
			// 保存其他生存者
			cleaned = append(cleaned, producer)
		} else {
			removed = true
		}
	}
	// Note: this leaves keys in the DB even if they have empty lists
	// 注意：这边只是删除生产者并没有删除key
	r.registrationMap[k] = cleaned
	return removed, len(cleaned)
}

// remove a Registration and all it's producers
// 删除Registration和它对应的Producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

// for what？？
func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// 查找Registrations
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		// 如果key和subkey都不是“*”,那么严格匹配
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k} // 找到就返回
		}
		return Registrations{} // 没有找到就返回空
	}

	// 如果key或者subkey存在“*”,那么模糊匹配
	results := Registrations{}
	for k := range r.registrationMap {
		// 判断是否匹配规则
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		// 如果符合规则就添加到results，等待返回
		results = append(results, k)
	}
	return results
}

// 根据category key subkey查找所有的Producer
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	// 如果key和subkey都不是“*”,那么严格匹配
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return r.registrationMap[k]
	}

	// 如果key或者subkey存在“*”,那么模糊匹配
	results := Producers{}
	for k, producers := range r.registrationMap {
		// 判断是否匹配规则
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		// 如果符合规则就添加到results，等待返回
		for _, producer := range producers {
			found := false
			for _, p := range results {
				if producer.peerInfo.id == p.peerInfo.id {
					found = true
				}
			}
			if found == false {
				results = append(results, producer)
			}
		}
	}
	return results
}

// 根据producer.peerInfo.id查找所属的registration(可能多个)
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		// 遍历每个registration下的producers
		for _, p := range producers {
			if p.peerInfo.id == id {
				results = append(results, k)
				break
			}
		}
	}
	return results
}

/*********************************************Registration************************************************************/

// 是否符合模糊匹配的规则
func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

/*********************************************Registrations************************************************************/

// 过滤获取所有与输入参数匹配的Registration
func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

// 获取MAP中所有Registration的key
func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

// 获取MAP中所有Registration的subkey
func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

/*********************************************Producers************************************************************/

// 获取所有可用的Producer
func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		// 判断是否可用的条件：
		// 	1: 现在距离上次更新的时间 < inactivityTimeout
		//	2: 没有被标记为墓碑状态
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

// 获取Producers中所有的PeerInfo
func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}
