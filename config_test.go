package mantil

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	// store current env
	currentEnv := make(map[string]string)
	for _, k := range mantilEnvVars {
		currentEnv[k] = os.Getenv(k)
		os.Unsetenv(k)
	}

	t.Run("", testKvTableName)
	t.Run("", testResourceTags)

	// reset env for other thests
	for k, v := range currentEnv {
		if v == "" {
			os.Unsetenv(k)
			continue
		}
		os.Setenv(k, v)
	}
}

func testKvTableName(t *testing.T) {
	nameFromEnv := "kv-test"
	os.Setenv(EnvKVTableName, nameFromEnv)

	expected, err := defaultConfig.KvTableName()
	require.NoError(t, err)
	require.Equal(t, expected, nameFromEnv)

	os.Unsetenv(EnvKVTableName)
	expected, err = defaultConfig.KvTableName()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(expected, "mantil-go-"))
	require.True(t, strings.HasSuffix(expected, "-unit"))

	args0 := os.Args[0]
	os.Args[0] = ""
	expected, err = defaultConfig.KvTableName()
	require.Error(t, err)

	os.Setenv(EnvStageName, "dev")
	expected, err = defaultConfig.KvTableName()
	require.Error(t, err)

	os.Setenv(EnvProjectName, "project1")
	expected, err = defaultConfig.KvTableName()
	require.NoError(t, err)
	require.Equal(t, expected, "project1-dev-kv")

	os.Args[0] = args0
}

func testResourceTags(t *testing.T) {
	os.Setenv(EnvProjectName, "project")
	os.Setenv(EnvStageName, "stage")
	os.Setenv("test", "test")

	tags := defaultConfig.ResourceTags()
	assert.Len(t, tags, 2)
	assert.Contains(t, tags, EnvProjectName)
	assert.Contains(t, tags, EnvStageName)
	assert.NotContains(t, tags, "test")

	os.Unsetenv("test")
}

func TestMain(m *testing.M) {
	logPanic = false
	os.Exit(m.Run())
}
