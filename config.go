package mantil

import (
	"fmt"
	"os"
	"os/user"
	"strings"
)

const (
	EnvProjectName = "MANTIL_PROJECT_NAME"
	EnvStageName   = "MANTIL_STAGE_NAME"
	EnvKVTableName = "MANTIL_KV_TABLE_NAME"
)

type Config struct {
}

func (c *Config) KvTableName() (string, error) {
	if val, ok := os.LookupEnv(EnvKVTableName); ok {
		return val, nil
	}
	if c.isUnitTestEnv() {
		return fmt.Sprintf("mantil-go-%s-unit", c.username()), nil
	}
	return "", fmt.Errorf("table name not found, please set environment variable %s", EnvKVTableName)
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

func (c *Config) stageName() string {
	if val, ok := os.LookupEnv(EnvStageName); ok {
		return val
	}
	// TODO what now
	return ""
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
