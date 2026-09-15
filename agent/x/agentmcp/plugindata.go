package agentmcp

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

// pluginDataHashLen is the number of hex characters of the root hash in
// a plugin data directory name. Changing it moves every plugin's data
// directory.
const pluginDataHashLen = 12

// PluginDataDir returns the data directory path,
// <home>/.coder/plugin-data/<name>-<hash>, for the plugin named name
// whose install directory is root. The hash covers the cleaned root, so
// two installations of the same plugin name get different directories.
// The directory is not created, and name is used as given, so it must be
// a single path element.
func PluginDataDir(home, name, root string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(root)))
	return filepath.Join(home, ".coder", "plugin-data", name+"-"+hex.EncodeToString(sum[:])[:pluginDataHashLen])
}
