package mantil

import (
	"fmt"
	"os"
	"os/user"
	"strings"
)

const (
	EnvProjectName = "MANTIL_PROJECT"
	EnvStageName   = "MANTIL_STAGE"
	EnvKVTableName = "MANTIL_KV_TABLE"
	EnvTagPrefix   = "MANTIL"
)

var mantilEnvVars = []string{EnvProjectName, EnvStageName, EnvKVTableName}

type Config struct {
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

// ResourceTags returns list of all tags that should be added to newly created resources
// which is all env variables with EnvTagPrefix prefix
func (c *Config) ResourceTags() map[string]string {
	tags := map[string]string{}
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, EnvTagPrefix) {
			continue
		}
		pair := strings.SplitN(e, "=", 2)
		t, v := pair[0], pair[1]
		tags[t] = v
	}
	return tags
}
