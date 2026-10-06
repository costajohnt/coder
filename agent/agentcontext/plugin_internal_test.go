package agentcontext

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasLinkInside(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require elevated privileges on Windows")
	}

	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	require.NoError(t, os.MkdirAll(filepath.Join(target, "sub"), 0o755))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "link")))
	canonReal, err := filepath.EvalSymlinks(target)
	require.NoError(t, err)

	canonDir := filepath.Dir(canonReal)

	assert.False(t, hasLinkInside(canonDir, filepath.Join(canonReal, "sub")))
	assert.True(t, hasLinkInside(canonDir, filepath.Join(canonDir, "link", "sub")), "symlinked ancestor below base")
	assert.True(t, hasLinkInside(canonDir, filepath.Join(canonReal, "missing")), "unreadable component")
	assert.True(t, hasLinkInside(filepath.Join(canonDir, "link"), filepath.Join(canonDir, "link")), "base that is itself a link")
	linkedSub := filepath.Join(canonDir, "link", "sub")
	assert.False(t, hasLinkInside(linkedSub, linkedSub), "links above base are trusted")
}
