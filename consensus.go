package consensus

import (
	"strings"
	"time"
)

type Log struct {
	Id      string   `bson:"_id"`
	Records []Record `bson:"records"`
	Err     error    `bson:"-"`
}

type Record struct {
	Id      string    `bson:"id"`
	PrevId  string    `bson:"prevId"`
	Payload []byte    `bson:"payload,omitempty"`
	Created time.Time `bson:"created"`
}

func NewPayload(logId, recordId string, payload []byte) Payload {
	return Payload{
		Id:      logId + "/" + recordId,
		Payload: payload,
	}
}

type Payload struct {
	Id      string `bson:"_id"`
	Payload []byte `bson:"payload"`
}

func (p Payload) RecordId() string {
	if idx := strings.LastIndex(p.Id, "/"); idx >= 0 {
		return p.Id[idx+1:]
	}
	return ""
}

func (p Payload) LogId() string {
	if idx := strings.LastIndex(p.Id, "/"); idx >= 0 {
		return p.Id[:idx]
	}
	return ""
}
