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
		Headers:  http.Header{"X-Awesome": []string{"a", "b"}},
		OID:      "someoidhere",
		Length:   837582,
		Userdata: &jsonguy,
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

func TestConditionalStore(t *testing.T) {
	existing := fileMeta{}
	header := http.Header{}
	if !shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected allow (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	if !shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected allow (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-None-Match", `*`)
	if !shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected allow (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-None-Match", `*`)
	if shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected deny (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-None-Match", `"a"`)
	if !shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected allow (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-None-Match", `"a"`)
	if shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected deny (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-None-Match", `"b"`)
	if !shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected allow (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-None-Match", `"b"`)
	if !shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected allow (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-None-Match", `"a", "b"`)
	if !shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected allow (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-None-Match", `"a", "b"`)
	if shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected deny (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-Match", `*`)
	if shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected deny (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-Match", `*`)
	if !shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected allow (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-Match", `"a"`)
	if shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected deny (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-Match", `"a"`)
	if !shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected allow (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-Match", `"b"`)
	if shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected deny (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-Match", `"b"`)
	if shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected deny (%v, OID:%q)", header, existing.OID)
	}

	existing = fileMeta{}
	header = http.Header{}
	header.Set("If-Match", `"a", "b"`)
	if shouldStoreMeta(header, false, existing) {
		t.Errorf("Expected deny (%v, nonexist)", header)
	}

	existing = fileMeta{}
	header = http.Header{}
	existing.OID = "a"
	header.Set("If-Match", `"a", "b"`)
	if !shouldStoreMeta(header, true, existing) {
		t.Errorf("Expected allow (%v, OID:%q)", header, existing.OID)
	}
}
