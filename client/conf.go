package cbfsclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/couchbaselabs/cbfs/config"
)

func getConfURL(uin string) (string, error) {
	u, err := url.Parse(uin)
	if err != nil {
		return "", err
	}

	u.Path = "/.cbfs/config/"
	return u.String(), nil
}

// Get the current configuration.
func GetConfig(u string) (cbfsconfig.CBFSConfig, error) {
	conf := cbfsconfig.CBFSConfig{}

	confu, err := getConfURL(u)
	if err != nil {
		return conf, err
	}

	res, err := http.Get(confu)
	if err != nil {
		return conf, err
	}
	defer res.Body.Close()
	d := json.NewDecoder(res.Body)
	err = d.Decode(&conf)
	if err != nil {
		return conf, err
	}
	return conf, nil
}

// Set a configuration parameter by name.
func SetConfigParam(u, key, val string) error {
	conf, err := GetConfig(u)
	if err != nil {
		return err
	}

	err = conf.SetParameter(key, val)
	if err != nil {
		return err
	}

	data, err := json.Marshal(&conf)
	if err != nil {
		return err
	}

	confu, err := getConfURL(u)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", confu,
		bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		bod := make([]byte, 512)
		l, _ := res.Body.Read(bod)
		return fmt.Errorf("HTTP Error: %v / %v", res.Status, bod[:l])
	}
	return nil
}
