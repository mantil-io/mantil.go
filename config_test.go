package mantil

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logPanic = false
	setUnitTestConfig(nil)
	os.Exit(m.Run())
}

func TestConfig(t *testing.T) {
	SetLogger(nil)

	var c cfg
	err := c.load()
	require.NoError(t, err)
	require.Equal(t, c.ResourceTags["key"], "value")
	require.Equal(t, c.WsForwarderName, "ws-forwarder")

	//buf, _ := json.Marshal(c)
	//fmt.Printf("%s", buf)
}

func TestKvTableName(t *testing.T) {
	nameFromEnv := "kv-test"
	t.Setenv(EnvKVTableName, nameFromEnv)

	expected, err := config().kvTableName()
	require.NoError(t, err)
	require.Equal(t, expected, nameFromEnv)

	os.Unsetenv(EnvKVTableName)
	expected, err = config().kvTableName()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(expected, "mantil-go-"))
	require.True(t, strings.HasSuffix(expected, "-unit"))

	args0 := os.Args[0]
	os.Args[0] = ""

	var c cfg
	c.NamingTemplate = "project-stage-%s-abcdef"
	expected, err = c.kvTableName()
	require.NoError(t, err)
	require.Equal(t, expected, "project-stage-kv-abcdef")

	os.Args[0] = args0
}

func setUnitTestConfig(t *testing.T) {
	c := &cfg{
		ResourceTags: map[string]string{
			"key": "value",
		},
		WsForwarderName: "ws-forwarder",
	}
	c.NamingTemplate = "mantil-go-" + c.username() + "-unit-%s"
	buf, _ := json.Marshal(c)
	e := base64.StdEncoding.EncodeToString(buf)
	if t == nil {
		os.Setenv(EnvConfig, e)
	} else {
		t.Setenv(EnvConfig, e)
	}
}
