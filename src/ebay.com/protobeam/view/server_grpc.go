package view

import (
	"fmt"
	"net"
	"os"
	"runtime/pprof"
	"time"

	"ebay.com/protobeam/table"
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
	ok, pending := s.p.check(r.Key, r.Start, r.Through, time.Duration(r.WaitTime)*time.Second)
	return &CheckResult{Ok: ok, Pending: pending}, nil
}

// DecideTx let a client indicate a tx result without waiting for the log to catch up.
func (s *serverGrpc) DecideTx(ctx context.Context, r *DecideTxRequest) (*DecideTxResult, error) {
	s.p.rpcDecideTx(&r.Dec)
	return &DecideTxResult{}, nil
}

func (s *serverGrpc) Stats(ctx context.Context, r *StatsRequest) (*StatsResult, error) {
	return s.p.Stats(), nil
}

func (s *serverGrpc) SampleKeys(ctx context.Context, r *SampleKeysRequest) (*SampleKeysResult, error) {
	keys := s.p.sampleKeys(r.MaxKeys)
	return &SampleKeysResult{Keys: keys}, nil
}

func (s *serverGrpc) Metrics(ctx context.Context, r *MetricsRequest) (*MetricsResult, error) {
	t := table.MetricsTable(s.p.metrics, time.Millisecond)
	res := MetricsResult{
		Rows: make([]*MetricsResult_Row, len(t)),
	}
	for i, tr := range t {
		res.Rows[i] = &MetricsResult_Row{Cells: tr}
	}
	return &res, nil
}

func (s *serverGrpc) Profile(ctx context.Context, r *ProfileRequest) (*ProfileResult, error) {
	f, err := os.Create(r.Filename)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Starting CPU profiling, to %s for %s\n", f.Name(), r.Duration)
	pprof.StartCPUProfile(f)
	go func() {
		time.Sleep(r.Duration)
		pprof.StopCPUProfile()
		fmt.Printf("Completed CPU profile to %s\n", f.Name())
	}()
	return &ProfileResult{}, nil
}

func (s *serverGrpc) KeyStats(ctx context.Context, r *KeyStatsRequest) (*KeyStatsResult, error) {
	stats := s.p.keyStats(r.BucketSize)
	return &KeyStatsResult{stats}, nil
}
