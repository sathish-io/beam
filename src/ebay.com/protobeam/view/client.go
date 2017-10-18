package view

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"ebay.com/protobeam/config"
)

func NewClient(c *config.Beam) *Client {
	return &Client{c}
}

type Client struct {
	cfg *config.Beam
}

func (c *Client) Fetch(key string) (string, int64, error) {
	p := url.Values{}
	p.Add("k", key)
	var res fetchResult
	err := c.get(c.partition(key), "/fetch", p, &res)
	return res.Value, res.Index, err
}

func (c *Client) FetchAt(key string, idx int64) (string, int64, error) {
	p := url.Values{}
	p.Add("k", key)
	p.Add("i", strconv.FormatInt(idx, 10))
	var res fetchResult
	err := c.get(c.partition(key), "/fetchAt", p, &res)
	return res.Value, res.Index, err
}

func (c *Client) Check(key string, start int64, through int64) (ok bool, pending bool, err error) {
	p := url.Values{}
	p.Add("k", key)
	p.Add("s", strconv.FormatInt(start, 10))
	p.Add("t", strconv.FormatInt(through, 10))
	var res checkResult
	err = c.get(c.partition(key), "/check", p, &res)
	return res.Ok, res.Pending, err
}

func (c *Client) Stats() ([]Stats, error) {
	numParts := len(c.cfg.Partitions)
	type res struct {
		stats Stats
		err   error
	}
	resCh := make(chan res, numParts)
	for p := 0; p < numParts; p++ {
		go func(p int) {
			var s Stats
			err := c.get(p, "/stats", nil, &s)
			resCh <- res{s, err}
		}(p)
	}
	results := make([]Stats, 0, numParts)
	var err error
	for p := 0; p < numParts; p++ {
		r := <-resCh
		if r.err != nil {
			err = r.err
		}
		results = append(results, r.stats)
	}
	sort.Slice(results, func(a, b int) bool {
		return results[a].Partition < results[b].Partition
	})
	return results, err
}

func (c *Client) partition(key string) int {
	return int(hash(key, uint32(len(c.cfg.Partitions))))
}

func (c *Client) get(partition int, path string, params url.Values, resBody interface{}) error {
	req, err := http.NewRequest(http.MethodGet, "http://"+c.cfg.Partitions[partition]+path+"?"+params.Encode(), nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return json.NewDecoder(res.Body).Decode(resBody)
}
