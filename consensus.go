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
	Payload []byte    `bson:"payload"`
	Created time.Time `bson:"created"`
}
