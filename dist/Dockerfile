FROM ubuntu:14.04

MAINTAINER Traun Leyden <tleyden@couchbase.com>

ENV GOPATH /opt/go
ENV PATH $GOPATH/bin:$PATH

# Get dependencies
RUN apt-get update && apt-get install -y \
  git \
  golang 

# Install cbfs
RUN go get -u -v -t github.com/couchbaselabs/cbfs

