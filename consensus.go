package consensus

import "time"

type Log struct {
	Id      string   `bson:"_id"`
	Records []Record `bson:"records"`
	Err     error    `bson:"-"`
}

type Record struct {
	Id          string    `bson:"id"`
	PrevId      string    `bson:"prevId"`
	Payload     []byte    `bson:"payload,omitempty"`     // Legacy: embedded payload, empty after migration
	PayloadHash string    `bson:"payloadHash,omitempty"` // New: SHA256 hash of payload
	Created     time.Time `bson:"created"`
}

// Payload represents a content-addressable payload stored separately
type Payload struct {
	Hash string `bson:"_id"` // SHA256 hash of Data
	Data []byte `bson:"data"`
}
