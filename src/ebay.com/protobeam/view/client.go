package view

import (
	"sort"
	"sync"
	"time"

	"ebay.com/protobeam/config"
	"ebay.com/protobeam/errors"
	"github.com/rcrowley/go-metrics"
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

func NewClient(c *config.Beam) (*Client, error) {
	client := Client{
		cfg: c,
		p:   make([]PartitionViewClient, len(c.Partitions)),
		m:   c.Metrics,
	}
	for p := range c.Partitions {
		conn, err := grpc.Dial(c.Partitions[p], grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		client.p[p] = NewPartitionViewClient(conn)
	}
	return &client, nil
}

type Client struct {
	cfg *config.Beam
	p   []PartitionViewClient
	m   metrics.Registry
}

func (c *Client) NumPartitions() int {
	return len(c.p)
}

func (c *Client) viewClient(partition int) PartitionViewClient {
	return c.p[partition]
}

func (c *Client) Fetch(key string) (string, int64, error) {
	mt := metrics.GetOrRegisterTimer("client.fetch", c.m)
	start := time.Now()
	defer mt.UpdateSince(start)

	pc := c.viewClient(c.partition(key))
	res, err := pc.Fetch(context.Background(), &FetchRequest{key})
	if err != nil {
		return "", 0, err
	}
	return res.Value, res.Index, nil
}

func (c *Client) FetchAt(key string, idx int64) (string, int64, error) {
	mt := metrics.GetOrRegisterTimer("client.fetchAt", c.m)
	start := time.Now()
	defer mt.UpdateSince(start)

	pc := c.viewClient(c.partition(key))
	res, err := pc.FetchAt(context.Background(), &FetchAtRequest{Key: key, Index: idx})
	if err != nil {
		return "", 0, err
	}
	return res.Value, res.Index, nil
}

func (c *Client) Check(key string, start int64, through int64, timeWait time.Duration) (ok bool, pending bool, err error) {
	mt := metrics.GetOrRegisterTimer("client.check", c.m)
	startTm := time.Now()
	defer mt.UpdateSince(startTm)

	pc := c.viewClient(c.partition(key))
	res, err := pc.Check(context.Background(), &CheckRequest{Key: key, Start: start, Through: through, WaitTime: timeWait})
	if err != nil {
		return false, false, err
	}
	return res.Ok, res.Pending, nil
}

func (c *Client) SampleKeys(maxKeys uint32) ([]string, error) {
	mt := metrics.GetOrRegisterTimer("client.sampleKeys", c.m)
	start := time.Now()
	defer mt.UpdateSince(start)

	samples := make([][]string, len(c.p))
	perrors := make([]error, len(c.p))
	var wg sync.WaitGroup
	wg.Add(len(c.p))
	for idx := range c.p {
		go func(idx int) {
			pres, err := c.p[idx].SampleKeys(context.Background(), &SampleKeysRequest{MaxKeys: maxKeys / uint32(len(c.p))})
			if err != nil {
				perrors[idx] = err
			} else {
				samples[idx] = pres.Keys
			}
			wg.Done()
		}(idx)
	}
	wg.Wait()
	total := 0
	for _, s := range samples {
		total += len(s)
	}
	// ensure keys are not returned in partition order, so that a subslice of keys should cross partitions
	res := make([]string, total)
	ridx := 0
	sidx := 0
	for ridx < len(res) {
		for _, s := range samples {
			res[ridx] = s[sidx]
			ridx++
			if ridx == len(res) {
				break
			}
		}
		sidx++
	}
	return res, errors.Any(perrors...)
}

func (c *Client) Metrics(p int) ([][]string, error) {
	m, err := c.viewClient(p).Metrics(context.Background(), &MetricsRequest{})
	if err != nil {
		return nil, err
	}
	t := make([][]string, len(m.Rows))
	for i, r := range m.Rows {
		t[i] = r.Cells
	}
	return t, nil
}

func (c *Client) Profile(p int, fn string, dur time.Duration) error {
	_, err := c.viewClient(p).Profile(context.Background(), &ProfileRequest{Filename: fn, Duration: dur})
	return err
}

func (c *Client) Stats() ([]StatsResult, error) {
	mt := metrics.GetOrRegisterTimer("client.stats", c.m)
	start := time.Now()
	defer mt.UpdateSince(start)

	numParts := len(c.cfg.Partitions)
	type res struct {
		stats *StatsResult
		err   error
	}
	resCh := make(chan res, numParts)
	for p := 0; p < numParts; p++ {
		go func(p int) {
			pc := c.viewClient(p)
			sr, err := pc.Stats(context.Background(), &StatsRequest{})
			resCh <- res{sr, err}
		}(p)
	}
	results := make([]StatsResult, 0, numParts)
	var err error
	for p := 0; p < numParts; p++ {
		r := <-resCh
		if r.err != nil {
			err = r.err
			continue
		}
		results = append(results, *r.stats)
	}
	sort.Slice(results, func(a, b int) bool {
		return results[a].Partition < results[b].Partition
	})
	return results, err
}

type KeyVersionStat struct {
	Versions uint32
	Keys     uint32
}

func (c *Client) KeyStats(bucketSize uint32) ([]KeyVersionStat, error) {
	type pres struct {
		val map[uint32]uint32
		err error
	}
	resCh := make(chan pres)
	statsReq := &KeyStatsRequest{BucketSize: bucketSize}
	for p := 0; p < len(c.cfg.Partitions); p++ {
		go func(p int) {
			s, err := c.viewClient(p).KeyStats(context.Background(), statsReq)
			if err != nil {
				resCh <- pres{err: err}
			} else {
				resCh <- pres{val: s.VersionCounts}
			}
		}(p)
	}
	var res map[uint32]uint32
	var err error
	for p := 0; p < len(c.cfg.Partitions); p++ {
		r := <-resCh
		if r.err != nil {
			err = r.err
		} else {
			if len(res) == 0 {
				res = r.val
			} else {
				for k, v := range r.val {
					res[k] += v
				}
			}
		}
	}
	orderedRes := make([]KeyVersionStat, len(res))
	i := 0
	for k, v := range res {
		orderedRes[i] = KeyVersionStat{k, v}
		i++
	}
	sort.Slice(orderedRes, func(a, b int) bool {
		return orderedRes[a].Versions < orderedRes[b].Versions
	})
	return orderedRes, err
}

func (c *Client) partition(key string) int {
	return int(hash(key, uint32(len(c.cfg.Partitions))))
}
