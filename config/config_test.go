package cbfsconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
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

func TestSetParam(t *testing.T) {
	conf := DefaultConfig()

	err := conf.SetParameter("nonexistent", "novalue")
	if err == nil {
		t.Errorf("Failed to recongize nonexistent value")
		t.Fail()
	}

	tests := []struct {
		param string
		val   string
		ptr   interface{}
		exp   interface{}
	}{
		{"hbfreq", "3m", &conf.HeartbeatFreq, 3 * time.Minute},
		{"hash", "sha1", &conf.Hash, "sha1"},
		{"minrepl", "3", &conf.MinReplicas, 3},
	}

	for _, test := range tests {
		err = conf.SetParameter(test.param, test.val)
		if err != nil {
			t.Errorf("Error in %v: %v", test.param, err)
			t.Fail()
		}
		got := reflect.Indirect(reflect.ValueOf(test.ptr)).Interface()
		if got != test.exp {
			t.Errorf("Expected %v, got %v for %v=%v",
				test.exp, got, test.param, test.val)
			t.Fail()
		}
	}
}
