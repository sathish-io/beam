package msg

import (
	"errors"
	"fmt"

	"gopkg.in/Shopify/sarama.v1"
)

type MsgType uint8

const (
	Write       MsgType = 'W'
	Transaction MsgType = 'T'
	Decision    MsgType = 'D'
)

type Parsed struct {
	Index   int64
	MsgType MsgType
	Body    PbMessage
}

func (w WriteKeyValueMessage) Encode() ([]byte, error) {
	return encode(Write, &w)
}

func (t TransactionMessage) Encode() ([]byte, error) {
	return encode(Transaction, &t)
}

func (d DecisionMessage) Encode() ([]byte, error) {
	return encode(Decision, &d)
}

type PbMessage interface {
	Unmarshal(dAtA []byte) error
	MarshalTo(dAtA []byte) (int, error)
	Size() (n int)
}

func Decode(m *sarama.ConsumerMessage) (Parsed, error) {
	if len(m.Value) == 0 {
		return Parsed{}, errors.New("Message value has zero length")
	}
	res := Parsed{
		Index:   m.Offset + 1,
		MsgType: MsgType(m.Value[0]),
	}
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
	err := res.Body.Unmarshal(m.Value[1:])
	return res, err
}

func encode(t MsgType, body PbMessage) ([]byte, error) {
	d := make([]byte, 1+body.Size())
	d[0] = uint8(t)
	_, err := body.MarshalTo(d[1:])
	return d, err
}
