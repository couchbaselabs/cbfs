FROM ubuntu:14.04

MAINTAINER Traun Leyden <tleyden@couchbase.com>

ENV GOPATH /opt/go
ENV PATH $GOPATH/bin:$PATH

ADD refresh-cbfs /usr/local/bin/

# Get dependencies
RUN apt-get update && apt-get install -y \
  git \
  golang \
  mercurial

# Install cbfs + client 
RUN refresh-cbfs


