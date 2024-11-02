// Package transport provides a Transport for github.com/hashicorp/raft over gRPC.
package transport

import (
	"errors"
	"sync"
	"time"

	pb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type Manager struct {
	localAddress     raft.ServerAddress
	dialOptions      []grpc.DialOption
	heartbeatTimeout time.Duration
	logger           Logger

	transportsMtx sync.Mutex
	transports    map[uint32]*raftAPI

	connectionsMtx      sync.Mutex
	connections         map[raft.ServerAddress]*conn
	connectionsRefCount map[raft.ServerAddress]int

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

type conn struct {
	clientConn *grpc.ClientConn
	client     pb.RaftTransportClient
	mtx        sync.Mutex
}

// New creates both components of raft-grpc-transport: a gRPC service and a Raft Transport.
func New(localAddress raft.ServerAddress, dialOptions []grpc.DialOption, options ...Option) *Manager {
	m := &Manager{
		localAddress:        localAddress,
		dialOptions:         dialOptions,
		logger:              nilLogger,
		transports:          map[uint32]*raftAPI{},
		connections:         map[raft.ServerAddress]*conn{},
		connectionsRefCount: map[raft.ServerAddress]int{},
		shutdownCh:          make(chan struct{}),
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// Register the RaftTransport gRPC service on a gRPC server.
func (m *Manager) Register(s grpc.ServiceRegistrar) {
	pb.RegisterRaftTransportServer(s, gRPCAPI{manager: m})
}

// Transport returns a raft.Transport that communicates over gRPC.
func (m *Manager) Transport(id uint32) raft.Transport {
	m.transportsMtx.Lock()
	defer m.transportsMtx.Unlock()

	result, ok := m.transports[id]
	if !ok {
		result = newRaftAPI(id, m)
		m.transports[id] = result
	}

	return result
}

func (m *Manager) Close() error {
	m.shutdownLock.Lock()
	defer m.shutdownLock.Unlock()

	if m.shutdown {
		return nil
	}

	close(m.shutdownCh)
	m.shutdown = true
	return m.disconnectAll()
}

func (m *Manager) getTransport(id uint32) (*raftAPI, bool) {
	m.transportsMtx.Lock()
	defer m.transportsMtx.Unlock()
	result, ok := m.transports[id]
	return result, ok
}

func (m *Manager) removeTransport(id uint32) {
	m.transportsMtx.Lock()
	defer m.transportsMtx.Unlock()
	delete(m.transports, id)
}

func (m *Manager) connect(target raft.ServerAddress) (*conn, error) {
	m.connectionsMtx.Lock()
	c, ok := m.connections[target]
	if !ok {
		c = &conn{}
		m.connections[target] = c
	}

	m.connectionsRefCount[target]++
	m.connectionsMtx.Unlock()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.clientConn == nil {
		conn, err := grpc.Dial(string(target), m.dialOptions...)
		if err != nil {
			return nil, err
		}
		c.clientConn = conn
		c.client = pb.NewRaftTransportClient(conn)
	}

	return c, nil
}

func (m *Manager) disconnect(target raft.ServerAddress) {
	var toClose *conn

	m.connectionsMtx.Lock()
	m.connectionsRefCount[target]--
	if m.connectionsRefCount[target] == 0 {
		toClose = m.connections[target]
		delete(m.connections, target)
	}
	m.connectionsMtx.Unlock()

	if toClose != nil {
		toClose.mtx.Lock() // waits for grpc.Dial call above to return
		defer toClose.mtx.Unlock()

		if err := toClose.clientConn.Close(); err != nil {
			m.logger(err, "Failed to close gRCP client conn", "target", target)
		}
	}
}

func (m *Manager) disconnectAll() error {
	m.connectionsMtx.Lock()
	defer m.connectionsMtx.Unlock()

	var errs []error
	for k, conn := range m.connections {
		conn.mtx.Lock() // Lock conn.mtx to ensure Dial() is complete

		err := conn.clientConn.Close()
		if err != nil {
			errs = append(errs, err)
		}
		conn.mtx.Unlock()

		delete(m.connections, k)
		delete(m.connectionsRefCount, k)
	}

	return errors.Join(errs...)
}
