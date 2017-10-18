package view

import (
	"testing"
)

func Test_FiddleAddr(t *testing.T) {
	r := fiddleAddr("localhost:1234")
	if r != "localhost:1234" {
		t.Errorf("Expecing fidlleAddr(localhost:1234) to return localhost:1234, but got %s", r)
	}
	r = fiddleAddr("10.11.12.13:1234")
	if r != ":1234" {
		t.Errorf("Expecting fiddleAddr(10.11.12.13:1234) to return :1234, but got %s", r)
	}
}
