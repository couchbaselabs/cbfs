package main

import (
	"log"

	// Alias this because we call our connection couchbase
	cb "github.com/couchbaselabs/go-couchbase"
)

var couchbase *cb.Bucket

func dbConnect() (*cb.Bucket, error) {
	log.Printf("Connecting to couchbase bucket %v at %v",
		*couchbaseBucket, *couchbaseServer)
	rv, err := cb.GetBucket(*couchbaseServer, "default", *couchbaseBucket)
	if err != nil {
		return nil, err
	}
	return rv, nil
}
