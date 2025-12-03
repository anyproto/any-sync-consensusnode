package consensus

import "time"

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

type Payload struct {
	Id      string `bson:"_id"`
	LogId   string `bson:"logId"`
	Payload []byte `bson:"payload"`
}
