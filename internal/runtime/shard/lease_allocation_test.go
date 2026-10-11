package shard

import (
	"testing"
	"time"
)

func TestTargetOwnedShardsUsesConfiguredReplicaCount(t *testing.T) {
	tests := []struct {
		name        string
		numShards   uint32
		replicas    uint32
		wantPerNode uint32
	}{
		{name: "default-sized shard set with three replicas", numShards: 4096, replicas: 3, wantPerNode: 1366},
		{name: "single replica owns every shard", numShards: 4096, replicas: 1, wantPerNode: 4096},
		{name: "non-divisible shard count rounds up", numShards: 10, replicas: 4, wantPerNode: 3},
		{name: "more replicas than shards still targets one lease", numShards: 1, replicas: 3, wantPerNode: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewLeaseManagerWithReplicaCountAndRenewInterval(
				nil,
				nil,
				"worker-test",
				tt.numShards,
				tt.replicas,
				time.Second,
				time.Millisecond,
			)
			if got := manager.targetOwnedShards(); got != tt.wantPerNode {
				t.Fatalf("targetOwnedShards() = %d, want %d", got, tt.wantPerNode)
			}
		})
	}
}

func TestLeaseManagerDefaultsInvalidReplicaCount(t *testing.T) {
	manager := NewLeaseManagerWithReplicaCountAndRenewInterval(
		nil,
		nil,
		"worker-test",
		10,
		0,
		time.Second,
		time.Millisecond,
	)
	if got, want := manager.replicaCount, uint32(3); got != want {
		t.Fatalf("replicaCount = %d, want default %d", got, want)
	}
}
