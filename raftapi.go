package transport

import (
	"context"
	"io"
	"strconv"
	"sync"
	"time"

	pb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/metadata"
)

// These are calls from the Raft engine that we need to send out over gRPC.
type raftAPI struct {
	id               uint32
	manager          *Manager
	rpcChan          chan raft.RPC
	heartbeatFuncMtx sync.Mutex
	heartbeatFunc    func(raft.RPC)
	connenctedMtx    sync.Mutex
	connected        map[raft.ServerAddress]bool
	shutdown         bool
	shutdownCh       chan struct{}
	shutdownLock     sync.Mutex
}

var _ raft.Transport = &raftAPI{}
var _ raft.WithClose = &raftAPI{}
var _ raft.WithPeers = &raftAPI{}
var _ raft.WithPreVote = &raftAPI{}

func newRaftAPI(id uint32, m *Manager) *raftAPI {
	return &raftAPI{
		id:         id,
		manager:    m,
		rpcChan:    make(chan raft.RPC),
		connected:  map[raft.ServerAddress]bool{},
		shutdownCh: make(chan struct{}),
	}
}

// Consumer returns a channel that can be used to consume and respond to RPC requests.
func (r *raftAPI) Consumer() <-chan raft.RPC {
	return r.rpcChan
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (r *raftAPI) LocalAddr() raft.ServerAddress {
	return r.manager.localAddress
}

func (r *raftAPI) getPeer(target raft.ServerAddress) (pb.RaftTransportClient, error) {
	c, err := r.manager.connect(target)
	if err != nil {
		return nil, err
	}

	r.connenctedMtx.Lock()
	r.connected[target] = true
	r.connenctedMtx.Unlock()

	return c.client, nil
}

// AppendEntries sends the appropriate RPC to the target node.
func (r *raftAPI) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	c, err := r.getPeer(target)
	if err != nil {
		return err
	}
	ctx := r.getRequestContext()
	if r.manager.heartbeatTimeout > 0 && isHeartbeat(args) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.manager.heartbeatTimeout)
		defer cancel()
	}
	ret, err := c.AppendEntries(ctx, encodeAppendEntriesRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeAppendEntriesResponse(ret)
	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (r *raftAPI) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	c, err := r.getPeer(target)
	if err != nil {
		return err
	}
	ret, err := c.RequestVote(r.getRequestContext(), encodeRequestVoteRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeRequestVoteResponse(ret)
	return nil
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (r *raftAPI) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	c, err := r.getPeer(target)
	if err != nil {
		return err
	}
	ret, err := c.TimeoutNow(r.getRequestContext(), encodeTimeoutNowRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeTimeoutNowResponse(ret)
	return nil
}

// RequestPreVote is the command used by a candidate to ask a Raft peer for a vote in an election.
func (r *raftAPI) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	c, err := r.getPeer(target)
	if err != nil {
		return err
	}
	ret, err := c.RequestPreVote(r.getRequestContext(), encodeRequestPreVoteRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeRequestPreVoteResponse(ret)
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (r *raftAPI) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, req *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	c, err := r.getPeer(target)
	if err != nil {
		return err
	}
	stream, err := c.InstallSnapshot(r.getRequestContext())
	if err != nil {
		return err
	}
	if err := stream.Send(encodeInstallSnapshotRequest(req)); err != nil {
		return err
	}
	var buf [16384]byte
	for {
		n, err := data.Read(buf[:])
		if err == io.EOF || (err == nil && n == 0) {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.InstallSnapshotRequest{
			Data: buf[:n],
		}); err != nil {
			return err
		}
	}
	ret, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	*resp = *decodeInstallSnapshotResponse(ret)
	return nil
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (r *raftAPI) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	c, err := r.getPeer(target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(r.getRequestContext())
	stream, err := c.AppendEntriesPipeline(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	rpa := &raftPipelineAPI{
		stream:     stream,
		cancel:     cancel,
		inflightCh: make(chan *appendFuture, 20),
		doneCh:     make(chan raft.AppendFuture, 20),
	}
	go rpa.receiver()
	return rpa, nil
}

type raftPipelineAPI struct {
	stream        pb.RaftTransport_AppendEntriesPipelineClient
	cancel        func()
	inflightChMtx sync.Mutex
	inflightCh    chan *appendFuture
	doneCh        chan raft.AppendFuture
}

// AppendEntries is used to add another request to the pipeline.
// The send may block which is an effective form of back-pressure.
func (r *raftPipelineAPI) AppendEntries(req *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	af := &appendFuture{
		start:   time.Now(),
		request: req,
		done:    make(chan struct{}),
	}
	if err := r.stream.Send(encodeAppendEntriesRequest(req)); err != nil {
		return nil, err
	}
	r.inflightChMtx.Lock()
	select {
	case <-r.stream.Context().Done():
	default:
		r.inflightCh <- af
	}
	r.inflightChMtx.Unlock()
	return af, nil
}

// Consumer returns a channel that can be used to consume
// response futures when they are ready.
func (r *raftPipelineAPI) Consumer() <-chan raft.AppendFuture {
	return r.doneCh
}

// Close closes the pipeline and cancels all inflight RPCs
func (r *raftPipelineAPI) Close() error {
	r.cancel()
	r.inflightChMtx.Lock()
	close(r.inflightCh)
	r.inflightChMtx.Unlock()
	return nil
}

func (r *raftPipelineAPI) receiver() {
	for af := range r.inflightCh {
		msg, err := r.stream.Recv()
		if err != nil {
			af.err = err
		} else {
			af.response = *decodeAppendEntriesResponse(msg)
		}
		close(af.done)
		r.doneCh <- af
	}
}

type appendFuture struct {
	raft.AppendFuture

	start    time.Time
	request  *raft.AppendEntriesRequest
	response raft.AppendEntriesResponse
	err      error
	done     chan struct{}
}

// Error blocks until the future arrives and then
// returns the error status of the future.
// This may be called any number of times - all
// calls will return the same value.
// Note that it is not OK to call this method
// twice concurrently on the same Future instance.
func (f *appendFuture) Error() error {
	<-f.done
	return f.err
}

// Start returns the time that the append request was started.
// It is always OK to call this method.
func (f *appendFuture) Start() time.Time {
	return f.start
}

// Request holds the parameters of the AppendEntries call.
// It is always OK to call this method.
func (f *appendFuture) Request() *raft.AppendEntriesRequest {
	return f.request
}

// Response holds the results of the AppendEntries call.
// This method must only be called after the Error
// method returns, and will only be valid on success.
func (f *appendFuture) Response() *raft.AppendEntriesResponse {
	return &f.response
}

// EncodePeer is used to serialize a peer's address.
func (r *raftAPI) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer is used to deserialize a peer's address.
func (r *raftAPI) DecodePeer(p []byte) raft.ServerAddress {
	return raft.ServerAddress(p)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
func (r *raftAPI) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	r.heartbeatFuncMtx.Lock()
	r.heartbeatFunc = cb
	r.heartbeatFuncMtx.Unlock()
}

func (r *raftAPI) Close() error {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if r.shutdown {
		return nil
	}

	close(r.shutdownCh)
	r.shutdown = true

	r.DisconnectAll()
	r.manager.removeTransport(r.id)
	return nil
}

func (r *raftAPI) Connect(target raft.ServerAddress, t raft.Transport) {
	_, err := r.getPeer(target)
	if err != nil {
		r.manager.logger(err, "Failed to connect", "target", target)
	}
}

func (r *raftAPI) Disconnect(target raft.ServerAddress) {
	r.manager.disconnect(target)

	r.connenctedMtx.Lock()
	delete(r.connected, target)
	r.connenctedMtx.Unlock()
}

func (r *raftAPI) DisconnectAll() {
	r.connenctedMtx.Lock()
	defer r.connenctedMtx.Unlock()

	for target := range r.connected {
		r.manager.disconnect(target)
	}

	r.connected = map[raft.ServerAddress]bool{}
}

func (r *raftAPI) getRequestContext() context.Context {
	ctx := context.Background()
	value := strconv.FormatUint(uint64(r.id), 10)
	return metadata.AppendToOutgoingContext(ctx, metadataKey, value)
}
