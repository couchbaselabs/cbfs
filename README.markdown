How do I get the stuff
======================

    go get github.com/couchbaselabs/cbfs

And you'll find the source in $GOPATH/src/pkg/github.com/couchbaselabs/cbfs

How do I build the stuff
========================

    cd $GOPATH/src/pkg/github.com/couchbaselabs/cbfs
    go build

How do I run the stuff
======================

    mkdir -p /tmp/something
    ./cbfs -nodeID=trond  \
           -bucket=cbfs \
           -couchbase=http://mango.hq.northscale.net:8091/
           -root=/tmp/something \
           -viewProxy

Then go to http://localhost:8484/monitor/
