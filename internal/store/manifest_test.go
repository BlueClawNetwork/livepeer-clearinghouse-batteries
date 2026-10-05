package store_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestIngestStoresManifestAndSequence(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 0, f.EventWith(t, "with", "10", testutil.PM, map[string]any{"manifest_id": "m-1"})))
	require.NoError(t, f.DB.Ingest(ctx, "events", 1, 0, f.EventWith(t, "old-signer", "10", testutil.PM, map[string]any{"manifest_id": nil})))
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 1, f.EventWith(t, "oversized", "10", testutil.PM, map[string]any{"manifest_id": strings.Repeat("m", 257)})))
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 2, []byte(`{bad`)))
	rows, err := f.DB.Rows(ctx, `SELECT event_id,manifest_id,ingest_sequence,status FROM usage_events ORDER BY ingest_sequence`)
	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, "m-1", rows[0]["manifest_id"])
	require.Equal(t, "", rows[1]["manifest_id"])
	require.Equal(t, "", rows[2]["manifest_id"])
	for i, row := range rows {
		require.Equal(t, int64(i+1), row["ingest_sequence"], "ingestion order across partitions and statuses")
		if i < 3 {
			require.Equal(t, "applied", row["status"])
		}
	}
	require.Equal(t, "quarantined", rows[3]["status"])
	usage, err := f.DB.List(ctx, "usage", "")
	require.NoError(t, err)
	require.Equal(t, "m-1", usage[0]["manifest_id"])
}

func TestManifestCostSumsAppliedEventsOnly(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	usd := []any{"0.000001", "0.000002", "0.000003"}
	for i, fee := range []string{"3", "4", "5"} {
		require.NoError(t, f.DB.Ingest(ctx, "events", 0, int64(i), f.EventWith(t, "m1-"+fee, fee, testutil.PM, map[string]any{"manifest_id": "m-1", "sequence_number": i, "num_tickets": i + 1, "billable_secs": 1.5, "computed_fee_usd": usd[i], "current_time_unix": 1_790_000_000_000 + int64(i)})))
	}
	// A quarantined event for the same manifest (wrong session binding) is excluded.
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 3, f.EventWith(t, "m1-bad", "100", testutil.PM, map[string]any{"manifest_id": "m-1", "session_id": "state-other"})))
	cost, err := f.DB.ManifestCost(ctx, "m-1")
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"manifest_id": "m-1", "allocation_id": f.Allocation, "api_key_id": f.KeyID, "payment_session_id": f.Session,
		"app": "test-app", "orchestrator": testutil.Orch, "pm_session_id": testutil.PM,
		"event_count": 3, "ticket_count": 6, "fee_wei": "12", "fee_usd": "0.000006", "billable_secs": "4.500",
		"sequence_first": int64(0), "sequence_last": int64(2), "first_signed_at_ms": int64(1_790_000_000_000), "last_signed_at_ms": int64(1_790_000_000_002),
	}, cost)

	// One event without a USD conversion leaves the USD sum empty.
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 4, f.EventWith(t, "m2-a", "1", testutil.PM, map[string]any{"manifest_id": "m-2", "computed_fee_usd": "0.5"})))
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 5, f.EventWith(t, "m2-b", "1", testutil.PM, map[string]any{"manifest_id": "m-2", "sequence_number": 1})))
	cost, err = f.DB.ManifestCost(ctx, "m-2")
	require.NoError(t, err)
	require.Equal(t, "", cost["fee_usd"])
	require.Equal(t, "2", cost["fee_wei"])

	_, err = f.DB.ManifestCost(ctx, "unknown")
	require.True(t, errors.Is(err, sql.ErrNoRows))
	_, err = f.DB.ManifestCost(ctx, "")
	require.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestManifestCostRejectsSessionSpan(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 0, f.EventWith(t, "first", "1", testutil.PM, map[string]any{"manifest_id": "m-1"})))
	other := f.Request
	other.State = &store.RemoteState{StateID: "state-2", PMSessionID: testutil.PM, OrchestratorAddress: testutil.Orch, App: "test-app", Type: "live"}
	decision, err := f.DB.Authorize(ctx, other)
	require.NoError(t, err)
	require.Equal(t, 200, decision.Status)
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 1, f.EventWith(t, "second", "1", testutil.PM, map[string]any{"manifest_id": "m-1", "session_id": "state-2", "auth_id": decision.AuthID})))
	_, err = f.DB.ManifestCost(ctx, "m-1")
	require.ErrorIs(t, err, store.ErrManagementConflict)
}

func TestCostEventsFeed(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	for i := range 4 {
		require.NoError(t, f.DB.Ingest(ctx, "events", i%2, int64(i/2), f.EventWith(t, "ok-"+string(rune('a'+i)), "7", testutil.PM, map[string]any{"manifest_id": "m-1", "sequence_number": i})))
	}
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 2, f.EventWith(t, "bad", "not-a-fee", testutil.PM, nil)))
	page, err := f.DB.CostEvents(ctx, 0, 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	first := page[0]
	require.Equal(t, int64(1), first["ingest_sequence"])
	require.Equal(t, "ok-a", first["event_id"])
	require.Equal(t, "applied", first["status"])
	require.Equal(t, "", first["error"])
	require.Equal(t, "m-1", first["manifest_id"])
	require.Equal(t, int64(0), first["partition"])
	require.Equal(t, int64(0), first["offset"])
	require.Equal(t, "request-ok-a", first["request_id"])
	require.Equal(t, f.Session, first["auth_id"])
	require.Equal(t, f.Allocation, first["allocation_id"])
	require.Equal(t, "test-app", first["app"])
	require.Equal(t, testutil.Orch, first["orchestrator"])
	require.Equal(t, testutil.PM, first["pm_session_id"])
	require.Equal(t, int64(0), first["sequence_number"])
	require.Equal(t, int64(1), first["num_tickets"])
	require.Equal(t, "7", first["computed_fee_wei"])
	require.Nil(t, first["computed_fee_usd"])
	require.Equal(t, "10", first["billable_secs"])
	require.Equal(t, "0", first["pixels"])
	require.Equal(t, first["current_time_unix"], first["signed_at_ms"])
	require.Less(t, first["previous_time_unix"].(int64), first["current_time_unix"].(int64))
	require.Equal(t, int64(1), page[1]["partition"])

	page, err = f.DB.CostEvents(ctx, 4, 10)
	require.NoError(t, err)
	require.Len(t, page, 1)
	quarantined := page[0]
	require.Equal(t, "quarantined", quarantined["status"])
	require.NotEqual(t, "", quarantined["error"])
	require.Nil(t, quarantined["allocation_id"])
	require.Nil(t, quarantined["sequence_number"])
	require.Nil(t, quarantined["computed_fee_wei"], "an unparseable fee is not exposed")
	page, err = f.DB.CostEvents(ctx, 5, 10)
	require.NoError(t, err)
	require.Empty(t, page)
	for _, tc := range []struct {
		after int64
		limit int
	}{{-1, 10}, {0, 0}, {0, 1001}} {
		_, err := f.DB.CostEvents(ctx, tc.after, tc.limit)
		require.ErrorIs(t, err, store.ErrInvalidManagementInput)
	}
}
