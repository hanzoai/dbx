// Copyright 2026 Hanzo AI. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dbx

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectFunc opens a *DB for the given file path.
// The default for SQLite is DefaultSQLiteConnect.
type ConnectFunc func(dbPath string) (*DB, error)

// DefaultSQLiteConnect opens a SQLite database with WAL mode,
// 10s busy timeout, and standard pragmas for concurrent access.
func DefaultSQLiteConnect(dbPath string) (*DB, error) {
	pragmas := "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=journal_size_limit(200000000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=temp_store(MEMORY)&_pragma=cache_size(-16000)"
	return Open("sqlite", dbPath+pragmas)
}

// Pool holds a dual-connection pair for one SQLite database.
// Reads use the concurrent connection (configurable MaxOpenConns),
// writes use the nonconcurrent connection (1 conn, serialized).
type Pool struct {
	Path          string
	Concurrent    *DB // reads
	Nonconcurrent *DB // writes (serialized)
	refCount      int64
	lastAccess    int64 // UnixNano, updated on Acquire
}

func (p *Pool) Acquire() {
	atomic.StoreInt64(&p.lastAccess, time.Now().UnixNano())
	atomic.AddInt64(&p.refCount, 1)
}
func (p *Pool) Release() { atomic.AddInt64(&p.refCount, -1) }
func (p *Pool) InUse() bool { return atomic.LoadInt64(&p.refCount) > 0 }

// LastAccess returns when this pool was last acquired.
func (p *Pool) LastAccess() time.Time {
	ns := atomic.LoadInt64(&p.lastAccess)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (p *Pool) Close() error {
	var firstErr error
	if p.Concurrent != nil {
		if err := p.Concurrent.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if p.Nonconcurrent != nil {
		if err := p.Nonconcurrent.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PoolConfig tunes the pool manager.
type PoolConfig struct {
	// MaxPools is the max number of open database file pools.
	// Default: 2000. Hard cap: 2000.
	MaxPools int

	// ReadConns per DB. Default: 4. Recommended: NumCPU.
	ReadConns int

	// ReadIdleConns per DB. Default: 2.
	ReadIdleConns int

	// NumShards for lock partitioning. Default: runtime.NumCPU(). Power of 2 recommended.
	NumShards int

	// IdleTimeout is how long a pool can sit unused before the sweeper closes it.
	// Default: 30s. Set to 0 to disable idle eviction.
	IdleTimeout time.Duration

	// SweepInterval is how often the idle sweeper runs. Default: 10s.
	SweepInterval time.Duration

	// Connect opens a *DB for a path. Default: DefaultSQLiteConnect.
	Connect ConnectFunc
}

const maxPoolsCap = 2000

func (c *PoolConfig) defaults() {
	if c.MaxPools <= 0 {
		c.MaxPools = 2000
	}
	if c.MaxPools > maxPoolsCap {
		c.MaxPools = maxPoolsCap
	}
	if c.ReadConns <= 0 {
		c.ReadConns = 4
	}
	if c.ReadIdleConns <= 0 {
		c.ReadIdleConns = 2
	}
	if c.NumShards <= 0 {
		c.NumShards = runtime.NumCPU()
		if c.NumShards < 1 {
			c.NumShards = 1
		}
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.SweepInterval == 0 {
		c.SweepInterval = 10 * time.Second
	}
	if c.Connect == nil {
		c.Connect = DefaultSQLiteConnect
	}
}

// PoolStats tracks pool manager metrics.
// Each field is cache-line padded to prevent false sharing.
type PoolStats struct {
	Hits          int64; _ [7]int64
	Misses        int64; _ [7]int64
	Evictions     int64; _ [7]int64
	IdleEvictions int64; _ [7]int64
	Opens         int64; _ [7]int64
	Errors        int64; _ [7]int64
}

// HitRate returns the cache hit ratio (0.0 to 1.0).
// Returns 0 if no requests have been made.
func (s *PoolStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

type poolShard struct {
	mu    sync.RWMutex
	pools map[string]*list.Element
	lru   *list.List
}

type lruEntry struct {
	key  string
	pool *Pool
}

func (s *poolShard) evictOneLocked() *Pool {
	for elem := s.lru.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*lruEntry)
		if !entry.pool.InUse() {
			s.lru.Remove(elem)
			delete(s.pools, entry.key)
			return entry.pool
		}
	}
	return nil
}

// evictIdleLocked removes all pools idle longer than cutoff.
// Returns pools to close (caller closes without lock held).
func (s *poolShard) evictIdleLocked(cutoff time.Time) []*Pool {
	var toClose []*Pool
	elem := s.lru.Back()
	for elem != nil {
		entry := elem.Value.(*lruEntry)
		prev := elem.Prev()
		if !entry.pool.InUse() && entry.pool.LastAccess().Before(cutoff) {
			s.lru.Remove(elem)
			delete(s.pools, entry.key)
			toClose = append(toClose, entry.pool)
		}
		elem = prev
	}
	return toClose
}

// PoolManager manages an LRU cache of SQLite connection pools.
//
// Sharded RWMutex design: cache hits use RLock (non-blocking),
// only misses and evictions take an exclusive lock. Each shard
// operates independently for minimal contention at scale.
//
// A background sweeper goroutine closes idle pools (configurable
// via IdleTimeout). The sweeper runs every SweepInterval.
//
// Services are stateless — any instance serves any tenant by
// loading the correct SQLite file from shared storage.
type PoolManager struct {
	shards []poolShard
	config PoolConfig
	stats  PoolStats
	done   chan struct{}
	wg     sync.WaitGroup
}

// NewPoolManager creates a sharded LRU pool manager and starts
// the idle eviction sweeper.
func NewPoolManager(config PoolConfig) *PoolManager {
	config.defaults()
	shards := make([]poolShard, config.NumShards)
	perShard := config.MaxPools / config.NumShards
	if perShard < 1 {
		perShard = 1
	}
	for i := range shards {
		shards[i] = poolShard{
			pools: make(map[string]*list.Element, perShard),
			lru:   list.New(),
		}
	}
	pm := &PoolManager{
		shards: shards,
		config: config,
		done:   make(chan struct{}),
	}
	if config.IdleTimeout > 0 {
		pm.wg.Add(1)
		go pm.sweepLoop()
	}
	return pm
}

func (m *PoolManager) sweepLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.sweepIdle()
		}
	}
}

func (m *PoolManager) sweepIdle() {
	cutoff := time.Now().Add(-m.config.IdleTimeout)
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		toClose := s.evictIdleLocked(cutoff)
		s.mu.Unlock()
		for _, p := range toClose {
			p.Close()
			atomic.AddInt64(&m.stats.IdleEvictions, 1)
			atomic.AddInt64(&m.stats.Evictions, 1)
		}
	}
}

func (m *PoolManager) shard(key string) *poolShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &m.shards[h.Sum32()%uint32(len(m.shards))]
}

// Get returns a connection pool for the given database path.
// Creates the pool if it doesn't exist. Caller MUST call pool.Release().
func (m *PoolManager) Get(dbPath string) (*Pool, error) {
	s := m.shard(dbPath)

	// Fast path: RLock for cache hit
	s.mu.RLock()
	if elem, ok := s.pools[dbPath]; ok {
		pool := elem.Value.(*lruEntry).pool
		pool.Acquire()
		s.mu.RUnlock()
		atomic.AddInt64(&m.stats.Hits, 1)
		return pool, nil
	}
	s.mu.RUnlock()

	// Slow path: exclusive lock for miss
	s.mu.Lock()

	// Double-check
	if elem, ok := s.pools[dbPath]; ok {
		s.lru.MoveToFront(elem)
		pool := elem.Value.(*lruEntry).pool
		pool.Acquire()
		s.mu.Unlock()
		atomic.AddInt64(&m.stats.Hits, 1)
		return pool, nil
	}

	pool, err := m.openPool(dbPath)
	if err != nil {
		s.mu.Unlock()
		atomic.AddInt64(&m.stats.Errors, 1)
		return nil, err
	}
	atomic.AddInt64(&m.stats.Opens, 1)

	// Two-phase eviction
	maxPerShard := m.config.MaxPools / len(m.shards)
	if maxPerShard < 1 {
		maxPerShard = 1
	}
	var toClose []*Pool
	for s.lru.Len() >= maxPerShard {
		evicted := s.evictOneLocked()
		if evicted == nil {
			break
		}
		toClose = append(toClose, evicted)
	}

	entry := &lruEntry{key: dbPath, pool: pool}
	elem := s.lru.PushFront(entry)
	s.pools[dbPath] = elem
	pool.Acquire()
	s.mu.Unlock()

	// Close evicted pools without lock
	for _, p := range toClose {
		p.Close()
		atomic.AddInt64(&m.stats.Evictions, 1)
	}

	atomic.AddInt64(&m.stats.Misses, 1)
	return pool, nil
}

func (m *PoolManager) openPool(dbPath string) (*Pool, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return nil, fmt.Errorf("create dir for %s: %w", dbPath, err)
	}

	concurrent, err := m.config.Connect(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open read pool %s: %w", dbPath, err)
	}
	concurrent.DB().SetMaxOpenConns(m.config.ReadConns)
	concurrent.DB().SetMaxIdleConns(m.config.ReadIdleConns)

	nonconcurrent, err := m.config.Connect(dbPath)
	if err != nil {
		concurrent.Close()
		return nil, fmt.Errorf("open write pool %s: %w", dbPath, err)
	}
	nonconcurrent.DB().SetMaxOpenConns(1)
	nonconcurrent.DB().SetMaxIdleConns(1)

	return &Pool{
		Path:          dbPath,
		Concurrent:    concurrent,
		Nonconcurrent: nonconcurrent,
		lastAccess:    time.Now().UnixNano(),
	}, nil
}

// Stats returns a metrics snapshot (lock-free atomic reads).
func (m *PoolManager) Stats() PoolStats {
	s := PoolStats{
		Hits:          atomic.LoadInt64(&m.stats.Hits),
		Misses:        atomic.LoadInt64(&m.stats.Misses),
		Evictions:     atomic.LoadInt64(&m.stats.Evictions),
		IdleEvictions: atomic.LoadInt64(&m.stats.IdleEvictions),
		Opens:         atomic.LoadInt64(&m.stats.Opens),
		Errors:        atomic.LoadInt64(&m.stats.Errors),
	}
	return s
}

// Len returns total open pools across all shards.
func (m *PoolManager) Len() int {
	total := 0
	for i := range m.shards {
		m.shards[i].mu.RLock()
		total += m.shards[i].lru.Len()
		m.shards[i].mu.RUnlock()
	}
	return total
}

// Close stops the idle sweeper and closes all pools. Call on shutdown.
func (m *PoolManager) Close() {
	// Signal sweeper to stop
	select {
	case <-m.done:
		// already closed
	default:
		close(m.done)
	}
	m.wg.Wait()

	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for _, elem := range s.pools {
			elem.Value.(*lruEntry).pool.Close()
		}
		s.pools = make(map[string]*list.Element)
		s.lru.Init()
		s.mu.Unlock()
	}
}
