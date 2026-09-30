package resources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/canonical/lxd/shared/api"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"github.com/canonical/microcluster/v3/internal/cluster"
	"github.com/canonical/microcluster/v3/internal/db/update"
	clusterDB "github.com/canonical/microcluster/v3/microcluster/db"
	"github.com/canonical/microcluster/v3/microcluster/types"
)

// heartbeatTestClient is a types.Client that only knows its URL.
type heartbeatTestClient struct {
	types.Client

	url *url.URL
}

func (c heartbeatTestClient) URL() *url.URL {
	return c.url
}

// Ensures sendHeartbeats only reads the shared cluster member map while heartbeats are being sent, and reports which
// members were sent a heartbeat. Run with -race to detect concurrent access to the map.
func TestSendHeartbeats(t *testing.T) {
	const members = 250
	const heartbeatInterval = 10 * time.Second

	pendingAddr := "10.0.1.0:8443"
	recentAddr := "10.0.0.0:8443"
	failingAddr := "10.0.0.1:8443"

	hbInfo := types.HeartbeatInfo{ClusterMembers: map[string]types.ClusterMember{}}
	clients := types.Clients{heartbeatTestClient{url: &url.URL{Host: pendingAddr}}}
	for i := range members {
		addr := fmt.Sprintf("10.0.%d.%d:8443", i/250, i%250)
		address, err := types.ParseAddrPort(addr)
		require.NoError(t, err)

		member := types.ClusterMember{
			ClusterMemberLocal: types.ClusterMemberLocal{Name: fmt.Sprintf("member-%d", i), Address: address},
			Role:               "spare",
		}

		if addr == recentAddr {
			member.LastHeartbeat = time.Now()
		}

		hbInfo.ClusterMembers[addr] = member
		clients = append(clients, heartbeatTestClient{url: &url.URL{Host: addr}})
	}

	before := time.Now()
	send := func(ctx context.Context, c types.Client, hbInfo types.HeartbeatInfo) error {
		// Encode the heartbeat as the real client does.
		_, err := json.Marshal(hbInfo)
		if err != nil {
			return err
		}

		if c.URL().Host == failingAddr {
			return errors.New("Unreachable")
		}

		return nil
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sent, err := sendHeartbeats(context.Background(), logger, clients, hbInfo, heartbeatInterval, send)
	require.NoError(t, err)

	require.Len(t, sent, members-2)
	require.NotContains(t, sent, pendingAddr)
	require.NotContains(t, sent, recentAddr)
	require.NotContains(t, sent, failingAddr)
	for addr, lastHeartbeat := range sent {
		require.Contains(t, hbInfo.ClusterMembers, addr)
		require.False(t, lastHeartbeat.Before(before), "Heartbeat time for %q is before the round began", addr)
		require.True(t, hbInfo.ClusterMembers[addr].LastHeartbeat.IsZero(), "Cluster member %q was modified while sending heartbeats", addr)
	}
}

// Ensures updateMemberHeartbeats writes only the heartbeat and role, and only for members where either changed.
func TestUpdateMemberHeartbeats(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Each connection to an in-memory database gets its own database, and total_changes() is per connection.
	db.SetMaxOpenConns(1)

	_, err = update.NewSchema().Schema().Ensure(ctx, db)
	require.NoError(t, err)

	err = clusterDB.PrepareStmts(db, false)
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	lastRound := time.Now().Add(-time.Minute)
	for i := range 4 {
		_, err = cluster.CreateCoreClusterMember(ctx, tx, cluster.CoreClusterMember{
			Name:           fmt.Sprintf("member-%d", i),
			Address:        fmt.Sprintf("10.0.0.%d:8443", i),
			Certificate:    fmt.Sprintf("test-cert-%d", i),
			SchemaInternal: 1,
			SchemaExternal: 1,
			Heartbeat:      lastRound,
			Role:           "voter",
		})
		require.NoError(t, err)
	}

	before, err := cluster.GetCoreClusterMembers(ctx, tx)
	require.NoError(t, err)

	// Build the heartbeat record from the database, as beginHeartbeat does.
	members := map[string]types.ClusterMember{}
	for _, member := range before {
		members[member.Address] = types.ClusterMember{
			ClusterMemberLocal: types.ClusterMemberLocal{Name: member.Name},
			Role:               string(member.Role),
			LastHeartbeat:      member.Heartbeat,
		}
	}

	now := time.Now()

	// member-0 was sent a heartbeat.
	member := members["10.0.0.0:8443"]
	member.LastHeartbeat = now
	members["10.0.0.0:8443"] = member

	// member-1 was not sent a heartbeat, but changed role.
	member = members["10.0.0.1:8443"]
	member.Role = "spare"
	members["10.0.0.1:8443"] = member

	// member-2 is unchanged, and member-3 is not part of this heartbeat round.
	delete(members, "10.0.0.3:8443")

	var changesBefore, changesAfter int
	err = tx.QueryRowContext(ctx, "SELECT total_changes()").Scan(&changesBefore)
	require.NoError(t, err)

	roleStatusMap, err := updateMemberHeartbeats(ctx, tx, members)
	require.NoError(t, err)

	err = tx.QueryRowContext(ctx, "SELECT total_changes()").Scan(&changesAfter)
	require.NoError(t, err)

	require.Equal(t, 2, changesAfter-changesBefore, "Unexpected number of rows written")
	require.Equal(t, map[string]types.RoleStatus{
		"member-0": {Old: "voter", New: "voter"},
		"member-1": {Old: "voter", New: "spare"},
		"member-2": {Old: "voter", New: "voter"},
	}, roleStatusMap)

	after, err := cluster.GetCoreClusterMembers(ctx, tx)
	require.NoError(t, err)
	require.Len(t, after, len(before))

	for i := range after {
		expected := before[i]
		switch expected.Name {
		case "member-0":
			require.True(t, after[i].Heartbeat.Equal(now), "Heartbeat of %q was not updated", expected.Name)
			expected.Heartbeat = after[i].Heartbeat
		case "member-1":
			expected.Role = "spare"
		}

		require.Equal(t, expected, after[i])
	}

	err = cluster.UpdateCoreClusterMemberHeartbeat(ctx, tx, -1, now, "voter")
	require.True(t, api.StatusErrorCheck(err, http.StatusNotFound), "Expected not found error, got: %v", err)
}
