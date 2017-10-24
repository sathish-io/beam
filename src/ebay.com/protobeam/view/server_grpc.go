package view

import (
	"fmt"
	"net"

	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

func startGrpcServer(addr string, p *Partition) error {
	addr = fiddleAddr(addr)
	fmt.Printf("Starting GRPPC server on %v\n", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s := serverGrpc{p}
	grpcServer := grpc.NewServer()
	RegisterPartitionViewServer(grpcServer, &s)
	go grpcServer.Serve(l)
	return nil
}

type serverGrpc struct {
	p *Partition
}

func (s *serverGrpc) makeFetchResult(val string, idx int64) *FetchResult {
	if idx == 0 {
		return &FetchResult{Exists: false}
	}
	return &FetchResult{
		Value:  val,
		Index:  idx,
		Exists: true,
	}
}

func (s *serverGrpc) Fetch(ctx context.Context, r *FetchRequest) (*FetchResult, error) {
	v, idx := s.p.fetch(r.Key)
	return s.makeFetchResult(v, idx), nil
}

func (s *serverGrpc) FetchAt(ctx context.Context, r *FetchAtRequest) (*FetchResult, error) {
	v, idx := s.p.fetchAt(r.Key, r.Index)
	return s.makeFetchResult(v, idx), nil
}

func (s *serverGrpc) Check(ctx context.Context, r *CheckRequest) (*CheckResult, error) {
	ok, pending := s.p.check(r.Key, r.Start, r.Through)
	return &CheckResult{Ok: ok, Pending: pending}, nil
}

func (s *serverGrpc) Stats(ctx context.Context, r *StatsRequest) (*StatsResult, error) {
	return s.p.Stats(), nil

}
