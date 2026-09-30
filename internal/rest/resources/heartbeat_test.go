package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/canonical/microcluster/v4/microcluster/types"
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
