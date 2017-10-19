package msg

type WriteKeyValueMessage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TransactionMessage struct {
	Cond   []Condition            `json:"cond"`
	Writes []WriteKeyValueMessage `json:"writes"`
}

type Condition struct {
	Key   string `json:"key"`
	Index int64  `json:"index"`
}

type DecisionMessage struct {
	Tx     int64 `json:"tx"`
	Commit bool  `json:"commit"`
}
