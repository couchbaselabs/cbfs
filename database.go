package main

import (
	"log"
	"net/http"

	// Alias this because we call our connection couchbase
	cb "github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/util"
)

var couchbase *cb.Bucket

const ddocKey = "/@ddocVersion"
const ddocVersion = 3
const designDoc = `
{
    "spatialInfos": [],
    "viewInfos": [
        {
            "map": "function (doc, meta) {\n  if (doc.type === \"file\") {\n    var toEmit = {};\n    toEmit[doc.oid] = meta.id;\n    if (doc.older) {\n      for (var i = 0; i < doc.older.length; i++) {\n        toEmit[doc.older[i].oid] = meta.id;\n      }\n    }\n    for (var k in toEmit) {\n      emit([k, \"file\", meta.id], null);\n    }\n  } else if (doc.type === \"blob\") {\n    var replicas=0;\n    for (var node in doc.nodes) {\n      replicas++;\n      emit([doc.oid, \"blob\", node], null);\n    }\n    if (replicas === 0) {\n      emit([doc.oid, \"blob\", \"\"], null);\n    }\n  }\n}",
            "name": "file_blobs",
            "removeLink": "#removeView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Ffile_blobs",
            "viewLink": "#showView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Ffile_blobs"
        },
        {
            "map": "function (doc, meta) {\n  if(doc.type == \"file\") {  \n    var idarr = meta.id.split(\"/\");\n    emit(idarr, doc.length);\n  }\n}",
            "name": "file_browse",
            "reduce": "_stats",
            "removeLink": "#removeView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Ffile_browse",
            "viewLink": "#showView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Ffile_browse"
        },
        {
            "map": "function (doc, meta) {\n  if (doc.type === 'blob') {\n    emit(doc.garbage ? 'garbage' : 'live', doc.length);\n  }\n}",
            "name": "garbage",
            "reduce": "_stats",
            "removeLink": "#removeView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Fgarbage",
            "viewLink": "#showView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Fgarbage"
        },
        {
            "map": "function (doc, meta) {\n  if (doc.type === \"blob\") {\n    for (var n in doc.nodes) {\n      emit(n, null);\n    }\n  }\n}",
            "name": "node_blobs",
            "reduce": "_count",
            "removeLink": "#removeView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Fnode_blobs",
            "viewLink": "#showView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Fnode_blobs"
        },
        {
            "map": "function (doc, meta) {\n  if (doc.type === \"node\") {\n    emit(meta.id.substring(1), 0);\n  } else if (doc.type === \"blob\") {\n    for (var n in doc.nodes) {\n      emit(n, doc.length);\n    }\n  }\n}",
            "name": "node_size",
            "reduce": "_sum",
            "removeLink": "#removeView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Fnode_size",
            "viewLink": "#showView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Fnode_size"
        },
        {
            "map": "function (doc, meta) {\n  if (doc.type === \"blob\" && !doc.garbage) {\n    var nreps = 0;\n    for (var x in doc.nodes) {\n      nreps++;\n    }\n    emit(nreps, null);\n  }\n}",
            "name": "repcounts",
            "reduce": "_count",
            "removeLink": "#removeView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Frepcounts",
            "viewLink": "#showView=cbfs%2F_design%252Fdev_cbfs%2F_view%2Frepcounts"
        }
    ],
    "views": {
        "file_blobs": {
            "map": "function (doc, meta) {\n  if (doc.type === \"file\") {\n    var toEmit = {};\n    toEmit[doc.oid] = doc.name ? doc.name : meta.id;\n    if (doc.older) {\n      for (var i = 0; i < doc.older.length; i++) {\n        toEmit[doc.older[i].oid] = doc.name ? doc.name : meta.id;\n      }\n    }\n    for (var k in toEmit) {\n      emit([k, \"file\", doc.name ? doc.name : meta.id], null);\n    }\n  } else if (doc.type === \"blob\") {\n    var replicas=0;\n    for (var node in doc.nodes) {\n      replicas++;\n      emit([doc.oid, \"blob\", node], null);\n    }\n    if (replicas === 0) {\n      emit([doc.oid, \"blob\", \"\"], null);\n    }\n  }\n}"
        },
        "file_browse": {
            "map": "function (doc, meta) {\n  if(doc.type == \"file\") {  \n    var idarr = (doc.name ? doc.name : meta.id).split(\"/\");\n    emit(idarr, doc.length);\n  }\n}",
            "reduce": "_stats"
        },
        "garbage": {
            "map": "function (doc, meta) {\n  if (doc.type === 'blob') {\n    emit(doc.garbage ? 'garbage' : 'live', doc.length);\n  }\n}",
            "reduce": "_stats"
        },
        "node_blobs": {
            "map": "function (doc, meta) {\n  if (doc.type === \"blob\") {\n    for (var n in doc.nodes) {\n      emit(n, null);\n    }\n  }\n}",
            "reduce": "_count"
        },
        "node_size": {
            "map": "function (doc, meta) {\n  if (doc.type === \"node\") {\n    emit(meta.id.substring(1), 0);\n  } else if (doc.type === \"blob\") {\n    for (var n in doc.nodes) {\n      emit(n, doc.length);\n    }\n  }\n}",
            "reduce": "_sum"
        },
        "repcounts": {
            "map": "function (doc, meta) {\n  if (doc.type === \"blob\" && !doc.garbage) {\n    var nreps = 0;\n    for (var x in doc.nodes) {\n      nreps++;\n    }\n    emit(nreps, null);\n  }\n}",
            "reduce": "_count"
        }
    }
}
`

func dbConnect() (*cb.Bucket, error) {
	cb.HTTPClient = &http.Client{
		Transport: TimeoutTransport(*viewTimeout),
	}

	log.Printf("Connecting to couchbase bucket %v at %v",
		*couchbaseBucket, *couchbaseServer)
	rv, err := cb.GetBucket(*couchbaseServer, "default", *couchbaseBucket)
	if err != nil {
		return nil, err
	}

	return rv, couchbaseutil.UpdateView(rv, "cbfs",
		ddocKey, designDoc, ddocVersion)
}
