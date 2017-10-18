package msg

import (
	"encoding/json"
	"errors"
	"fmt"

	"gopkg.in/Shopify/sarama.v1"
)

type MsgType uint8

const Write MsgType = 'W'
const Transaction MsgType = 'T'
const Decision MsgType = 'D'

type Parsed struct {
	Index   int64
	MsgType MsgType
	Body    interface{}
}

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

func Decode(m *sarama.ConsumerMessage) (Parsed, error) {
	if len(m.Value) == 0 {
		return Parsed{}, errors.New("Message value has zero length")
	}
	res := Parsed{
		Index:   m.Offset,
		MsgType: MsgType(m.Value[0]),
	}
	var err error
	switch res.MsgType {
	case Write:
		res.Body = new(WriteKeyValueMessage)
	case Transaction:
		res.Body = new(TransactionMessage)
	case Decision:
		res.Body = new(DecisionMessage)
	default:
		return res, fmt.Errorf("Unexpected message type of '%v'", res.MsgType)
	}
	err = json.Unmarshal(m.Value[1:], res.Body)
	return res, err
}
