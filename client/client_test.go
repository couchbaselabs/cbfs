package cbfsclient

import (
	"io"
	"log"
	"os"
	"testing"

	"github.com/tleyden/fakehttp"
)

func TestPathGen(t *testing.T) {
	c, err := New("http://cbfs:8484/")
	if err != nil {
		t.Fatalf("Error parsing thing: %v", err)
	}

	tests := map[string]string{
		"":    "http://cbfs:8484/",
		"a":   "http://cbfs:8484/a",
		"/a":  "http://cbfs:8484/a",
		"//a": "http://cbfs:8484/a",
	}

	for i, exp := range tests {
		p := c.URLFor(i)
		if p != exp {
			t.Errorf("Expected %q for %q, got %q",
				exp, i, p)
		}
	}
}

func TestRandomNode(t *testing.T) {

	testServer := fakehttp.NewHTTPServerWithPort(8484)
	testServer.Start()

	jsonContentType := map[string]string{"Content-Type": "application/json"}

	// when cbfs tries to query list of nodes, return empty value
	testServer.Response(200, jsonContentType, `{}`)

	c, err := New("http://localhost:8484/")
	if err != nil {
		t.Fatalf("Error parsing thing: %v", err)
	}

	_, _, err = c.RandomNode()
	log.Printf("err: %v", err)
	if err == nil {
		// since we don't have any nodes, RandomNode() should
		// return an error
		t.Fatalf("Expected error calling randomnode: %v", err)
	}

}

// Some assertions around filehandle's applicability
func TestTypes(t *testing.T) {
	_ = os.FileInfo(&FileHandle{})
	_ = io.Closer(&FileHandle{})
	_ = io.Reader(&FileHandle{})
	_ = io.ReaderAt(&FileHandle{})
	_ = io.WriterTo(&FileHandle{})
	_ = io.Seeker(&FileHandle{})
}
