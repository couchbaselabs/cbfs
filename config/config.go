package cbfsconfig

import (
	"time"

	"github.com/couchbaselabs/go-couchbase"
)

const dbKey = "/@globalConfig"

// Cluster-wide configuration
type CBFSConfig struct {
	// Frequency of Object GC Process
	GCFreq time.Duration `json:"gcfreq"`
	// Hash algorithm to use
	Hash string `json:"hash"`
	// Expected heartbeat frequency
	HeartbeatFreq time.Duration `json:"hbfreq"`
	// Minimum number of replicas to try to keep
	MinReplicas int `json:"minrepl"`
	// Number of blobs to remove from a stale node per period
	NodeCleanCount int `json:"cleanCount"`
	// Reconciliation frequency
	ReconcileFreq time.Duration `json:"reconcileFreq"`
	// How often to check for stale nodes
	StaleNodeCheckFreq time.Duration `json:"nodeCheckFreq"`
	// Time since the last heartbeat at which we consider a node stale
	StaleNodeLimit time.Duration `json:"staleLimit"`
}

// Get the default configuration
func DefaultConfig() CBFSConfig {
	return CBFSConfig{
		GCFreq:             time.Minute * 5,
		Hash:               "sha1",
		HeartbeatFreq:      time.Second * 5,
		MinReplicas:        3,
		NodeCleanCount:     1000,
		ReconcileFreq:      time.Hour * 24,
		StaleNodeCheckFreq: time.Minute,
		StaleNodeLimit:     time.Minute * 10,
	}
}

// Update this config within a bucket.
func (conf CBFSConfig) StoreConfig(db *couchbase.Bucket) error {
	return db.Set(dbKey, &conf)
}

// Update this config from the db.
func (conf *CBFSConfig) RetrieveConfig(db *couchbase.Bucket) error {
	return db.Get(dbKey, conf)
}
