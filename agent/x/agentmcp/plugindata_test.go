package agentmcp_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coder/coder/v2/agent/x/agentmcp"
)

func TestPluginDataDir(t *testing.T) {
	t.Parallel()

	home := filepath.FromSlash("/home/coder")
	root := filepath.FromSlash("/work/plugins/demo")

	got := agentmcp.PluginDataDir(home, "demo", root)
	assert.Equal(t, filepath.Join(home, ".coder", "plugin-data"), filepath.Dir(got))
	assert.Regexp(t, `^demo-[0-9a-f]{12}$`, filepath.Base(got))
	if runtime.GOOS != "windows" {
		// The returned path is part of the on-disk layout.
		assert.Equal(t, "/home/coder/.coder/plugin-data/demo-0875721e7ad7", got)
	}

	assert.Equal(t, got, agentmcp.PluginDataDir(home, "demo", root+string(filepath.Separator)),
		"a trailing separator names the same root")
	assert.NotEqual(t, got, agentmcp.PluginDataDir(home, "demo", filepath.FromSlash("/work/other/demo")),
		"two roots with the same plugin name must not share a directory")
}
