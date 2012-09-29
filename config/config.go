package cbfsconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/couchbaselabs/go-couchbase"
)

const dbKey = "/@globalConfig"

var unhandledValue = errors.New("Unsupported parameter")

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
	// How many objects to move when doing a replication check
	ReplicationCheckLimit int `json:"replicaCheckLimit"`
	// Default number of versions of a file to keep.
	DefaultVersionCount int `json:"defaultVersionCount"`
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
		ReplicationCheckLimit: 10000,
		DefaultVersionCount:   0,
	}
}

func jsonFieldName(sf reflect.StructField) string {
	fieldName := sf.Tag.Get("json")
	if fieldName == "" {
		fieldName = sf.Name
	}
	return fieldName
}

// Basically, vanilla marshaling, but return the durations in their
// string forms.
func (conf CBFSConfig) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}

	val := reflect.ValueOf(conf)
	for i := 0; i < val.NumField(); i++ {
		v := (interface{})(val.Field(i).Interface())
		if x, ok := v.(time.Duration); ok {
			v = x.String()
		}

		m[jsonFieldName(val.Type().Field(i))] = v
	}

	return json.Marshal(m)
}

// And here's how you undo the above.
func (conf *CBFSConfig) UnmarshalJSON(data []byte) error {
	m := map[string]interface{}{}
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	for k, v := range m {
		err = conf.SetParameter(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// Set a parameter by name.
func (conf *CBFSConfig) SetParameter(name string, inval interface{}) error {
	var err error
	d := time.Duration(0)

	val := reflect.Indirect(reflect.ValueOf(conf))

	for i := 0; i < val.NumField(); i++ {
		sf := val.Type().Field(i)
		if jsonFieldName(sf) != name {
			continue
		}

		switch {
		case sf.Type == reflect.TypeOf(d):
			switch i := inval.(type) {
			case string:
				d, err = time.ParseDuration(i)
				if err != nil {
					return err
				}
			case float64:
				d = time.Duration(i)
			}
			val.Field(i).SetInt(int64(d))
			return nil
		case sf.Type.Kind() == reflect.String:
			val.Field(i).SetString(inval.(string))
			return nil
		case sf.Type.Kind() == reflect.Int:
			v := int64(0)
			switch i := inval.(type) {
			case string:
				v, err = strconv.ParseInt(i, 10, 64)
				if err != nil {
					return err
				}

			case float64:
				v = int64(i)
			}
			val.Field(i).SetInt(v)
			return nil
		default:
			return fmt.Errorf("Unhandled type in field %v", name)
		}
	}
	return unhandledValue
}

// Dump a text representation of this config to the given writer.
func (conf CBFSConfig) Dump(w io.Writer) {
	tw := tabwriter.NewWriter(w, 2, 4, 1, ' ', 0)
	val := reflect.ValueOf(conf)
	for i := 0; i < val.NumField(); i++ {
		fmt.Fprintf(tw, "%v:\t%v\n", jsonFieldName(val.Type().Field(i)),
			val.Field(i).Interface())
	}
	tw.Flush()
}

// Update this config within a bucket.
func (conf CBFSConfig) StoreConfig(db *couchbase.Bucket) error {
	return db.Set(dbKey, 0, &conf)
}

// Update this config from the db.
func (conf *CBFSConfig) RetrieveConfig(db *couchbase.Bucket) error {
	return db.Get(dbKey, conf)
}
