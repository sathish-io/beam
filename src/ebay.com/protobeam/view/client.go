package view

import (
	"encoding/json"
	"net/http"
	"net/url"
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
	err := c.get(key, "/fetch", p, &res)
	return res.Value, res.Index, err
}

func (c *Client) FetchAt(key string, idx int64) (string, int64, error) {
	p := url.Values{}
	p.Add("k", key)
	p.Add("i", strconv.FormatInt(idx, 10))
	var res fetchResult
	err := c.get(key, "/fetchAt", p, &res)
	return res.Value, res.Index, err
}

func (c *Client) Check(key string, start int64, through int64) (ok bool, pending bool, err error) {
	p := url.Values{}
	p.Add("k", key)
	p.Add("s", strconv.FormatInt(start, 10))
	p.Add("t", strconv.FormatInt(through, 10))
	var res checkResult
	err = c.get(key, "/check", p, &res)
	return res.Ok, res.Pending, err
}

func (c *Client) get(partitionKey string, path string, params url.Values, resBody interface{}) error {
	pIdx := hash(partitionKey, uint32(len(c.cfg.Partitions)))
	req, err := http.NewRequest(http.MethodGet, "http://"+c.cfg.Partitions[pIdx]+path+"?"+params.Encode(), nil)
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
