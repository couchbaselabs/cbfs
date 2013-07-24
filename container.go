package main

import (
	"net/http"
)

type Container struct {
}

func getContainer(req *http.Request) *Container {
	return &Container{}
}
