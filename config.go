package mantil

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"strings"
)

// Configuration environment variables:
const (
	EnvConfig      = "MANTIL_GO_CONFIG"
	EnvKVTableName = "MANTIL_KV_TABLE"
)

type cfg struct {
	ResourceTags    map[string]string
	WsForwarderName string
	NamingTemplate  string
}

func (c *cfg) load() error {
	if c.isUnitTestEnv() {
		c.NamingTemplate = "mantil-go-" + c.username() + "-unit-%s"
		c.ResourceTags = map[string]string{
			"unit-test-tag": "value",
		}
		return nil
	}
	encoded, err := ensureEnv(EnvConfig, "mantil.go config not found")
	if err != nil {
		return err
	}
	buf, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, &c); err != nil {
		return err
	}
	return nil
}

func (c cfg) kvTableName() (string, error) {
	val, _ := ensureEnv(EnvKVTableName, "kv table name not found")
	if val != "" {
		return val, nil
	}
	if c.isUnitTestEnv() {
		return fmt.Sprintf("mantil-go-%s-unit", c.username()), nil
	}
	return fmt.Sprintf(c.NamingTemplate, "kv"), nil
}

// hackery trick to know if I'm running in `go test`
// ref: https://stackoverflow.com/questions/14249217/how-do-i-know-im-running-within-go-test
func (c cfg) isUnitTestEnv() bool {
	return strings.HasSuffix(os.Args[0], ".test")
}

func ensureEnv(envVarName, errPrefix string) (string, error) {
	if val, ok := os.LookupEnv(envVarName); ok {
		return val, nil
	}
	return "", fmt.Errorf("%s, please set environment variable %s", errPrefix, envVarName)
}

func (c cfg) username() string {
	user, err := user.Current()
	if err != nil {
		return "anonymous"
	}
	return user.Username
}
