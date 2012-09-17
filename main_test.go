package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestFileMetaRoundTrip(t *testing.T) {
	jsonguy := json.RawMessage([]byte(`{"test":"I am a bucket!"}`))

	fmin := fileMeta{
		http.Header{"X-Awesome": []string{"a", "b"}},
		"someoidhere",
		837582,
		&jsonguy,
	}

	d, err := json.Marshal(fmin)
	if err != nil {
		t.Fatalf("Can't marshal %v: %v", fmin, err)
	}

	fmout := fileMeta{}
	err = json.Unmarshal(d, &fmout)
	if err != nil {
		t.Fatalf("Can't unmarshal %s: %v", d, err)
	}

	if !reflect.DeepEqual(fmin, fmout) {
		t.Fatalf("Didn't round trip to the same thing:\n%#v\n%#v",
			fmin, fmout)
	}
}

func TestFileMetaRoundNoJSON(t *testing.T) {
	fmin := fileMeta{
		Headers: http.Header{"X-Awesome": []string{"a", "b"}},
		OID:     "someoidhere",
		Length:  837582,
	}

	d, err := json.Marshal(fmin)
	if err != nil {
		t.Fatalf("Can't marshal %v: %v", fmin, err)
	}

	fmout := fileMeta{}
	err = json.Unmarshal(d, &fmout)
	if err != nil {
		t.Fatalf("Can't unmarshal %s: %v", d, err)
	}

	if !reflect.DeepEqual(fmin, fmout) {
		t.Fatalf("Didn't round trip to the same thing:\n%#v\n%#v",
			fmin, fmout)
	}
}
