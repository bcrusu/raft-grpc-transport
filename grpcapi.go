package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	pb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// These are requests incoming over gRPC that we need to relay to the Raft engine.

const (
	metadataKey = "raft-id"
)

type gRPCAPI struct {
	manager *Manager

	// "Unsafe" to ensure compilation fails if new methods are added but not implemented
	pb.UnsafeRaftTransportServer
}

func (g gRPCAPI) handleRPC(ctx context.Context, command interface{}, data io.Reader) (interface{}, error) {
	id, err := getMetadataID(ctx)
	if err != nil {
		return nil, err
	}

	transport, ok := g.manager.getTransport(id)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Unknown Raft instance %d.", id)
	}

	ch := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  command,
		RespChan: ch,
		Reader:   data,
	}
	if isHeartbeat(command) {
		// We can take the fast path and use the heartbeat callback and skip the queue in transport.rpcChan
		transport.heartbeatFuncMtx.Lock()
		fn := transport.heartbeatFunc
		transport.heartbeatFuncMtx.Unlock()
		if fn != nil {
			fn(rpc)
			goto wait
		}
	}
	select {
	case transport.rpcChan <- rpc:
	case <-transport.shutdownCh:
		return nil, raft.ErrTransportShutdown
	case <-g.manager.shutdownCh:
		return nil, raft.ErrTransportShutdown
	}

wait:
	select {
	case resp := <-ch:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Response, nil
	case <-transport.shutdownCh:
		return nil, raft.ErrTransportShutdown
	case <-g.manager.shutdownCh:
		return nil, raft.ErrTransportShutdown
	}
}

func (g gRPCAPI) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp, err := g.handleRPC(ctx, decodeAppendEntriesRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse)), nil
}

func (g gRPCAPI) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	resp, err := g.handleRPC(ctx, decodeRequestVoteRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeRequestVoteResponse(resp.(*raft.RequestVoteResponse)), nil
}

func (g gRPCAPI) TimeoutNow(ctx context.Context, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	resp, err := g.handleRPC(ctx, decodeTimeoutNowRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeTimeoutNowResponse(resp.(*raft.TimeoutNowResponse)), nil
}

func (g gRPCAPI) RequestPreVote(ctx context.Context, req *pb.RequestPreVoteRequest) (*pb.RequestPreVoteResponse, error) {
	resp, err := g.handleRPC(ctx, decodeRequestPreVoteRequest(req), nil)
	if err != nil {
		return nil, err
	}
	return encodeRequestPreVoteResponse(resp.(*raft.RequestPreVoteResponse)), nil
}

func (g gRPCAPI) InstallSnapshot(s pb.RaftTransport_InstallSnapshotServer) error {
	isr, err := s.Recv()
	if err != nil {
		return err
	}
	resp, err := g.handleRPC(s.Context(), decodeInstallSnapshotRequest(isr), &snapshotStream{s, isr.GetData()})
	if err != nil {
		return err
	}
	return s.SendAndClose(encodeInstallSnapshotResponse(resp.(*raft.InstallSnapshotResponse)))
}

type snapshotStream struct {
	s pb.RaftTransport_InstallSnapshotServer

	buf []byte
}

func (s *snapshotStream) Read(b []byte) (int, error) {
	if len(s.buf) > 0 {
		n := copy(b, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	m, err := s.s.Recv()
	if err != nil {
		return 0, err
	}
	n := copy(b, m.GetData())
	if n < len(m.GetData()) {
		s.buf = m.GetData()[n:]
	}
	return n, nil
}

func (g gRPCAPI) AppendEntriesPipeline(s pb.RaftTransport_AppendEntriesPipelineServer) error {
	for {
		msg, err := s.Recv()
		if err != nil {
			return err
		}
		resp, err := g.handleRPC(s.Context(), decodeAppendEntriesRequest(msg), nil)
		if err != nil {
			// TODO(quis): One failure doesn't have to break the entire stream?
			// Or does it all go wrong when it's out of order anyway?
			return err
		}
		if err := s.Send(encodeAppendEntriesResponse(resp.(*raft.AppendEntriesResponse))); err != nil {
			return err
		}
	}
}

func isHeartbeat(command interface{}) bool {
	req, ok := command.(*raft.AppendEntriesRequest)
	if !ok {
		return false
	}
	return req.Term != 0 && len(req.Addr) != 0 && req.PrevLogEntry == 0 && req.PrevLogTerm == 0 && len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}

func getMetadataID(ctx context.Context) (uint32, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, errors.New("metadata is missing")
	}

	values, ok := md[metadataKey]
	if !ok {
		return 0, errors.New("metadata does not contain raft-id")
	}

	if len(values) > 1 {
		return 0, errors.New("metadata contains multiple raft-id values")
	}

	id, err := strconv.ParseUint(values[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse raft-id %q", values[0])
	}

	return uint32(id), nil
}
