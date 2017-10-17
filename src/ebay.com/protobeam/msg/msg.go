package msg

import (
	"encoding/json"
	"errors"
	"fmt"

	"gopkg.in/Shopify/sarama.v1"
)

type MsgType uint8

const Write MsgType = 'W'

type Parsed struct {
	Index   int64
	MsgType MsgType
	Body    interface{}
}

type WriteKeyValueMessage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func Decode(m *sarama.ConsumerMessage) (Parsed, error) {
	if len(m.Value) == 0 {
		return Parsed{}, errors.New("Message value has zero length")
	}
	res := Parsed{
		Index:   m.Offset,
		MsgType: MsgType(m.Value[0]),
	}
	if res.MsgType == Write {
		var body WriteKeyValueMessage
		err := json.Unmarshal(m.Value[1:], &body)
		res.Body = body
		return res, err
	}
	return res, fmt.Errorf("Unexpected message type of '%v'", res.MsgType)
}
