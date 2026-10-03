package trust

import (
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/canonical/lxd/shared"
	"github.com/stretchr/testify/require"

	"github.com/canonical/microcluster/v3/microcluster/types"
)

// testMember returns a cluster member with a certificate of its own.
func testMember(t *testing.T, name string, address string) types.ClusterMember {
	t.Helper()

	certPEM, _, err := shared.GenerateMemCert(false, shared.CertOptions{})
	require.NoError(t, err)

	cert, err := types.ParseX509Certificate(string(certPEM))
	require.NoError(t, err)

	return types.ClusterMember{
		ClusterMemberLocal: types.ClusterMemberLocal{
			Name:        name,
			Address:     types.AddrPort{AddrPort: netip.MustParseAddrPort(address)},
			Certificate: *cert,
		},
	}
}

// TestReplaceLeavesUnchangedRemotes checks that replacing the remotes with the list already on disk, as every
// heartbeat does, writes nothing, and that a changed, new or departed member still reaches the disk.
func TestReplaceLeavesUnchangedRemotes(t *testing.T) {
	dir := t.TempDir()
	remotes := &Remotes{data: map[string]types.Remote{}}

	one := testMember(t, "one", "10.0.0.1:9443")
	two := testMember(t, "two", "10.0.0.2:9443")
	three := testMember(t, "three", "10.0.0.3:9443")

	require.NoError(t, remotes.Replace(dir, one, two, three))

	stat := func(name string) os.FileInfo {
		info, err := os.Stat(filepath.Join(dir, name+".yaml"))
		require.NoError(t, err)

		return info
	}

	// A write replaces the file by renaming a new one over it, so an untouched file is the same file.
	before := map[string]os.FileInfo{"one": stat("one"), "two": stat("two"), "three": stat("three")}

	require.NoError(t, remotes.Replace(dir, one, two, three))
	for name, info := range before {
		require.True(t, os.SameFile(info, stat(name)), "remote %q was rewritten though unchanged", name)
	}

	// One member changes address, one leaves, one joins.
	two.Address = types.AddrPort{AddrPort: netip.MustParseAddrPort("10.0.0.22:9443")}
	four := testMember(t, "four", "10.0.0.4:9443")
	require.NoError(t, remotes.Replace(dir, one, two, four))

	require.True(t, os.SameFile(before["one"], stat("one")), "remote \"one\" was rewritten though unchanged")
	require.False(t, os.SameFile(before["two"], stat("two")), "remote \"two\" changed but was not rewritten")
	require.NoFileExists(t, filepath.Join(dir, "three.yaml"))
	require.FileExists(t, filepath.Join(dir, "four.yaml"))

	// What is on disk is what the remotes hold.
	loaded := &Remotes{data: map[string]types.Remote{}}
	require.NoError(t, loaded.Load(dir))
	require.Equal(t, remotes.RemoteAddresses(), loaded.RemoteAddresses())
	require.Equal(t, "10.0.0.22:9443", loaded.RemoteAddresses()["two"].String())
}
