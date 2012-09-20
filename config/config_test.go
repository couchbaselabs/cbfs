package cbfsconfig

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestJSONRoundTrip(t *testing.T) {
	conf := DefaultConfig()
	d, err := json.Marshal(&conf)
	if err != nil {
		t.Fatalf("Error marshaling config: %v", err)
	}

	conf2 := CBFSConfig{}
	err = json.Unmarshal(d, &conf2)
	if err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	if !reflect.DeepEqual(conf, conf2) {
		t.Fatalf("Unmarshalled value is different:\n%v\n%v", conf, conf2)
	}
}
