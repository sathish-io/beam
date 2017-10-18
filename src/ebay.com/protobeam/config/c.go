package config

import (
	"bufio"
	"encoding/json"
	"os"
)

type Beam struct {
	BrokerList []string `json:"brokers"`    // the list of kafka brokers in the cluster
	Partitions []string `json:"partitions"` // the list of partition servers in partition order, implies total number of partitions
	Partition  int      `json:"partition"`  // what partition should this process host? [can be overridden by -p flag], set to -1 to not run a partition view
}

func Load(fn string) (*Beam, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := Beam{Partition: -1}
	err = json.NewDecoder(bufio.NewReader(f)).Decode(&r)
	return &r, err
}
