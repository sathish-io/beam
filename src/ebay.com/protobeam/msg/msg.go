package msg

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/ugorji/go/codec"
	"gopkg.in/Shopify/sarama.v1"
)

type MsgType uint8

const Write MsgType = 'W'
const Transaction MsgType = 'T'
const Decision MsgType = 'D'

var jsonHandle = codec.JsonHandle{}
var bincHandle = codec.BincHandle{}

var encPool = sync.Pool{
	New: func() interface{} {
		return codec.NewEncoder(ioutil.Discard, &bincHandle)
	},
}

var decPool = sync.Pool{
	New: func() interface{} {
		return codec.NewDecoderBytes(nil, &bincHandle)
	},
}

type Parsed struct {
	Index   int64
	MsgType MsgType
	Body    interface{}
}

func (w WriteKeyValueMessage) Encode() ([]byte, error) {
	return encode(Write, w)
}

func (t TransactionMessage) Encode() ([]byte, error) {
	return encode(Transaction, t)
}

func (d DecisionMessage) Encode() ([]byte, error) {
	return encode(Decision, d)
}

func Decode(m *sarama.ConsumerMessage) (Parsed, error) {
	if len(m.Value) == 0 {
		return Parsed{}, errors.New("Message value has zero length")
	}
	res := Parsed{
		Index:   m.Offset + 1,
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
	dec := decPool.Get().(*codec.Decoder)
	dec.ResetBytes(m.Value[1:])
	err = dec.Decode(res.Body)
	decPool.Put(dec)
	return res, err
}

func encode(t MsgType, body interface{}) ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte(uint8(t))
	enc := encPool.Get().(*codec.Encoder)
	enc.Reset(&b)
	err := enc.Encode(body)
	encPool.Put(enc)
	return b.Bytes(), err
}
