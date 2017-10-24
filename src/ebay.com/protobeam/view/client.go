package view

import (
	"sort"
	"sync"

	"ebay.com/protobeam/config"
	"ebay.com/protobeam/errors"
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

func NewClient(c *config.Beam) (*Client, error) {
	client := Client{
		cfg: c,
		p:   make([]PartitionViewClient, len(c.Partitions)),
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
}

func (c *Client) viewClient(partition int) PartitionViewClient {
	return c.p[partition]
}

func (c *Client) Fetch(key string) (string, int64, error) {
	pc := c.viewClient(c.partition(key))
	res, err := pc.Fetch(context.Background(), &FetchRequest{key})
	if err != nil {
		return "", 0, err
	}
	return res.Value, res.Index, nil
}

func (c *Client) FetchAt(key string, idx int64) (string, int64, error) {
	pc := c.viewClient(c.partition(key))
	res, err := pc.FetchAt(context.Background(), &FetchAtRequest{Key: key, Index: idx})
	if err != nil {
		return "", 0, err
	}
	return res.Value, res.Index, nil
}

func (c *Client) Check(key string, start int64, through int64) (ok bool, pending bool, err error) {
	pc := c.viewClient(c.partition(key))
	res, err := pc.Check(context.Background(), &CheckRequest{Key: key, Start: start, Through: through})
	if err != nil {
		return false, false, err
	}
	return res.Ok, res.Pending, nil
}

func (c *Client) SampleKeys(maxKeys uint32) ([]string, error) {
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
	res := samples[0]
	for p := 1; p < len(c.p); p++ {
		res = append(res, samples[p]...)
	}
	return res, errors.Any(perrors...)
}

func (c *Client) Stats() ([]StatsResult, error) {
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

func (c *Client) partition(key string) int {
	return int(hash(key, uint32(len(c.cfg.Partitions))))
}
