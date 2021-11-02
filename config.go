package mantil

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"strings"
)

const (
	EnvConfig      = "MANTIL_GO_CONFIG"
	EnvProjectName = "MANTIL_PROJECT"
	EnvStageName   = "MANTIL_STAGE"
	EnvKVTableName = "MANTIL_KV_TABLE"
	EnvTagPrefix   = "MANTIL"
)

var mantilEnvVars = []string{EnvProjectName, EnvStageName, EnvKVTableName}

type Config struct {
	ResourceTags    map[string]string
	WsForwarderName string
}

func readConfig() (Config, error) {
	c := Config{}
	encoded, err := ensureEnv(EnvConfig, "mantil.go config not found")
	if err != nil {
		return c, err
	}
	buf, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(buf, &c); err != nil {
		return c, err
	}
	return c, nil
}

func (c *Config) KvTableName() (string, error) {
	val, err := ensureEnv(EnvKVTableName, "kv table name not found")
	if val != "" {
		return val, nil
	}
	if c.isUnitTestEnv() {
		return fmt.Sprintf("mantil-go-%s-unit", c.username()), nil
	}
	stage, err := ensureEnv(EnvStageName, "stage name not found")
	if err != nil {
		return "", err
	}
	project, err := ensureEnv(EnvProjectName, "project name not found")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s-kv", project, stage), nil
}

// hackery trick to know if I'm running in `go test`
// ref: https://stackoverflow.com/questions/14249217/how-do-i-know-im-running-within-go-test
func (c *Config) isUnitTestEnv() bool {
	return strings.HasSuffix(os.Args[0], ".test")
}

func (c *Config) isUserDevEnv() bool {
	// TODO
	return true
}

func (c *Config) isIntegrationEnv() bool {
	// TODO w
	return true
}

func (c *Config) stageName() (string, error) {
	if val, ok := os.LookupEnv(EnvStageName); ok {
		return val, nil
	}
	return "", fmt.Errorf("stage name not found, please set environment variable %s", EnvStageName)
}

func ensureEnv(envVarName, errPrefix string) (string, error) {
	if val, ok := os.LookupEnv(envVarName); ok {
		return val, nil
	}
	return "", fmt.Errorf("%s, please set environment variable %s", errPrefix, envVarName)
}

func (c *Config) projectName() string {
	if val, ok := os.LookupEnv(EnvProjectName); ok {
		return val
	}
	// TODO what now
	return ""
}

func (c *Config) username() string {
	user, err := user.Current()
	if err != nil {
		return "anonymous"
	}
	return user.Username
}
