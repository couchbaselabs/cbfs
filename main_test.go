package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func fmEq(a, b fileMeta) bool {
	return a.OID == b.OID &&
		a.Length == b.Length &&
		a.Modified.Equal(b.Modified) &&
		reflect.DeepEqual(a.Headers, b.Headers) &&
		((a.Userdata == nil && b.Userdata == nil) ||
			(reflect.DeepEqual(*a.Userdata, *b.Userdata)))
}

func TestFileMetaRoundTrip(t *testing.T) {
	jsonguy := json.RawMessage([]byte(`{"test":"I am a bucket!"}`))
	now := time.Now()

	fmin := fileMeta{
		http.Header{"X-Awesome": []string{"a", "b"}},
		"someoidhere",
		837582,
		&jsonguy,
		now,
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

	if !fmEq(fmin, fmout) {
		t.Fatalf("Didn't round trip to the same thing:\n%#v\n%#v",
			fmin, fmout)
	}
}

func TestFileMetaRoundNoJSON(t *testing.T) {
	now := time.Now()

	fmin := fileMeta{
		Headers:  http.Header{"X-Awesome": []string{"a", "b"}},
		OID:      "someoidhere",
		Length:   837582,
		Modified: now,
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

	if !fmEq(fmin, fmout) {
		t.Fatalf("Didn't round trip to the same thing:\n%#v\n%#v",
			fmin, fmout)
	}
}
