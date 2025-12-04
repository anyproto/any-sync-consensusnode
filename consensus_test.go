package consensus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPayload(t *testing.T) {
	pl := NewPayload("logId", "recordId", []byte{1, 2, 3})
	assert.Equal(t, "logId", pl.LogId())
	assert.Equal(t, "recordId", pl.RecordId())

	pl = NewPayload("l/ogI/d/", "recordId", []byte{1, 2, 3})
	assert.Equal(t, "l/ogI/d/", pl.LogId())
	assert.Equal(t, "recordId", pl.RecordId())
}
