package mantil

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigIsUnitTestEnv(t *testing.T) {
	require.True(t, defaultConfig.isUnitTestEnv())
	//fmt.Printf("args %v\n", os.Args)
	//
	beforeTestEnv, _ := os.LookupEnv(EnvKVTableName)

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

	if beforeTestEnv != "" {
		os.Setenv(EnvKVTableName, beforeTestEnv)
	}

	//fmt.Printf("table name: %s\n", expected)
}

func TestMain(m *testing.M) {
	logPanic = false
	os.Exit(m.Run())
}
