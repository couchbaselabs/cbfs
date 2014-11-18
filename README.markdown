

How do I get the stuff
======================

    go get github.com/couchbaselabs/cbfs

And you'll find the source in
`$GOPATH/src/github.com/couchbaselabs/cbfs` (and a `cbfs` binary
should be in your path)

How do I build the stuff
========================

```
cd $GOPATH/src/pkg/github.com/couchbaselabs/cbfs
go build
```

How do I run the stuff
======================

```
mkdir -p /tmp/localdata
./cbfs -nodeID=$mynodeid \
       -bucket=cbfs \
       -couchbase=http://$mycouchbaseserver:8091/
       -root=/tmp/localdata \
       -viewProxy
```

The server will be empty at this point, you can install the monitor
using cbfsclient (`go get github.com/couchbaselabs/cbfs/tools/cbfsclient`)

```
cbfsclient http://localhost:8484/ upload \
    $GOPATH/src/github.com/couchbaselabs/cbfs/monitor monitor
```

Then go to [http://localhost:8484/monitor/](http://localhost:8484/monitor/)

Running on Docker / CoreOS
==========================

See [Traun Leyden](https://github.com/tleyden)'s blog post on [Running CBFS + Couchbase Cluster on CoreOS](http://tleyden.github.io/blog/2014/11/14/running-cbfs/).
