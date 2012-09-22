package cbfsconfig

import (
	"fmt"
	"io"
	"reflect"
	"text/tabwriter"
	"time"

	"github.com/couchbaselabs/go-couchbase"
)

const dbKey = "/@globalConfig"

// Cluster-wide configuration
type CBFSConfig struct {
	// Frequency of Object GC Process
	GCFreq time.Duration `json:"gcfreq"`
	// Maximum number of items to look for in a GC pass.
	GCLimit int `json:"gclimit"`
	// Hash algorithm to use
	Hash string `json:"hash"`
	// Expected heartbeat frequency
	HeartbeatFreq time.Duration `json:"hbfreq"`
	// Minimum number of replicas to try to keep
	MinReplicas int `json:"minrepl"`
	// Maximum number of replicas to try to keep
	MaxReplicas int `json:"maxrepl"`
	// Number of blobs to remove from a stale node per period
	NodeCleanCount int `json:"cleanCount"`
	// Reconciliation frequency
	ReconcileFreq time.Duration `json:"reconcileFreq"`
	// How often to check for stale nodes
	StaleNodeCheckFreq time.Duration `json:"nodeCheckFreq"`
	// Time since the last heartbeat at which we consider a node stale
	StaleNodeLimit time.Duration `json:"staleLimit"`
	// How often to check for underreplication
	UnderReplicaCheckFreq time.Duration `json:"underReplicaCheckFreq"`
	// How long to check for overreplication
	OverReplicaCheckFreq time.Duration `json:"overReplicaCheckFreq"`
}

// Get the default configuration
func DefaultConfig() CBFSConfig {
	return CBFSConfig{
		GCFreq:                time.Minute * 5,
		GCLimit:               5000,
		Hash:                  "sha1",
		HeartbeatFreq:         time.Second * 5,
		MinReplicas:           3,
		MaxReplicas:           5,
		NodeCleanCount:        1000,
		ReconcileFreq:         time.Hour * 24,
		StaleNodeCheckFreq:    time.Minute,
		StaleNodeLimit:        time.Minute * 10,
		UnderReplicaCheckFreq: time.Minute * 5,
		OverReplicaCheckFreq:  time.Minute * 10,
	}
}

// Dump a text representation of this config to the given writer.
func (conf CBFSConfig) Dump(w io.Writer) {
	tw := tabwriter.NewWriter(w, 2, 4, 1, ' ', 0)
	val := reflect.ValueOf(conf)
	for i := 0; i < val.NumField(); i++ {
		sf := val.Type().Field(i)
		fieldName := sf.Tag.Get("json")
		if fieldName == "" {
			fieldName = sf.Name
		}

		fmt.Fprintf(tw, "%v:\t%v\n", fieldName, val.Field(i).Interface())
	}
	tw.Flush()
}

// Update this config within a bucket.
func (conf CBFSConfig) StoreConfig(db *couchbase.Bucket) error {
	return db.Set(dbKey, &conf)
}

// Update this config from the db.
func (conf *CBFSConfig) RetrieveConfig(db *couchbase.Bucket) error {
	return db.Get(dbKey, conf)
}
