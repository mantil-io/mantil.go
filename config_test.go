package mantil

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logPanic = false
	os.Exit(m.Run())
}

func TestConfig(t *testing.T) {
	SetLogger(nil)
	t.Setenv(EnvConfig, "eyJSZXNvdXJjZVRhZ3MiOnsiTUFOVElMX0tFWSI6ImFhN3Z6eWkiLCJNQU5USUxfUFJPSkVDVCI6InNpZ251cCIsIk1BTlRJTF9TVEFHRSI6InByb2R1Y3Rpb24iLCJNQU5USUxfV09SS1NQQUNFIjoiaWFuaWMifSwiV3NGb3J3YXJkZXJOYW1lIjoic2lnbnVwLXByb2R1Y3Rpb24td3MtZm9yd2FyZGVyLWFhN3Z6eWkifQ==")

	var c cfg
	err := c.load()
	require.NoError(t, err)
	require.Equal(t, c.ResourceTags["MANTIL_KEY"], "aa7vzyi")
	require.Equal(t, c.WsForwarderName, "signup-production-ws-forwarder-aa7vzyi")

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
