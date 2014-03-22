package cbfsconfig

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"text/tabwriter"
	"time"
)

type unhandledValue string

func (u unhandledValue) Error() string {
	return fmt.Sprintf("unhandled value: %q", string(u))
}

// Cluster-wide configuration
type CBFSConfig struct {
	// Frequency of Object GC Process
	GCFreq time.Duration `json:"gcfreq"`
	// Is garbage collection enabled?
	GCEnabled bool `json:"gcEnabled"`
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
	// Reconciliation object age
	ReconcileAge time.Duration `json:"reconcileAge"`
	// Quick reconciliation frequency
	QuickReconcileFreq time.Duration `json:"quickReconcileFreq"`
	// How often to verify we have all the blobs for which we're registered
	LocalValidationFreq time.Duration `json:"localValidationFreq"`
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
	// How often to update the node sizes
	UpdateNodeSizesFreq time.Duration `json:"updateSizesFreq"`
	// How frequently to free space from full nodes
	TrimFullNodesFreq time.Duration `json:"trimFullFreq"`
	// How many items to move from full nodes.
	TrimFullNodesCount int `json:"trimFullCount"`
	// How much space to keep free on nodes.
	TrimFullNodesSpace int64 `json:"trimFullSize"`
	// How far time can drift from DB before warning
	DriftWarnThresh time.Duration `json:"driftWarnThresh"`
}

// Get the default configuration
func DefaultConfig() CBFSConfig {
	return CBFSConfig{
		GCFreq:                time.Hour * 8,
		GCLimit:               5000,
		Hash:                  "sha1",
		HeartbeatFreq:         time.Second * 5,
		MinReplicas:           3,
		MaxReplicas:           5,
		NodeCleanCount:        5000,
		ReconcileFreq:         time.Hour * 24 * 7,
		ReconcileAge:          time.Hour * 24 * 30,
		QuickReconcileFreq:    time.Hour * 27,
		LocalValidationFreq:   time.Hour * 31,
		StaleNodeCheckFreq:    time.Minute,
		StaleNodeLimit:        time.Minute * 10,
		UnderReplicaCheckFreq: time.Minute * 5,
		OverReplicaCheckFreq:  time.Minute * 10,
		ReplicationCheckLimit: 10000,
		DefaultVersionCount:   0,
		UpdateNodeSizesFreq:   time.Second * 5,
		TrimFullNodesFreq:     time.Hour,
		TrimFullNodesCount:    10000,
		TrimFullNodesSpace:    1 * 1024 * 1024 * 1024,
		DriftWarnThresh:       5 * time.Minute,
	}
}

func jsonFieldName(sf reflect.StructField) string {
	fieldName := sf.Tag.Get("json")
	if fieldName == "" {
		fieldName = sf.Name
	}
	return fieldName
}

func (conf CBFSConfig) ToMap() map[string]interface{} {
	m := map[string]interface{}{}

	val := reflect.ValueOf(conf)
	for i := 0; i < val.NumField(); i++ {
		v := (interface{})(val.Field(i).Interface())
		if x, ok := v.(time.Duration); ok {
			v = x.String()
		}

		m[jsonFieldName(val.Type().Field(i))] = v
	}

	return m
}

// Basically, vanilla marshaling, but return the durations in their
// string forms.
func (conf CBFSConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(conf.ToMap())
}

// And here's how you undo the above.
func (conf *CBFSConfig) UnmarshalJSON(data []byte) error {
	m := map[string]interface{}{}
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	// Shove in all the defaults
	*conf = DefaultConfig()

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
		case sf.Type.Kind() == reflect.Bool:
			v := false
			switch i := inval.(type) {
			case string:
				v, err = strconv.ParseBool(i)
				if err != nil {
					return err
				}

			case bool:
				v = i
			}
			val.Field(i).SetBool(v)
			return nil
		case sf.Type.Kind() == reflect.String:
			val.Field(i).SetString(inval.(string))
			return nil
		case sf.Type.Kind() == reflect.Int, sf.Type.Kind() == reflect.Int64:
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
	return unhandledValue(name)
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
