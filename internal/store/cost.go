package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/livepeer/clearinghouse/internal/units"
)

// CostEvents returns stored usage events after a feed position, in ingestion order.
// Every status is included; session, allocation, and sequence fields are only set for applied events.
func (s *Store) CostEvents(ctx context.Context, after int64, limit int) ([]map[string]any, error) {
	if after < 0 {
		return nil, invalidInput("after must not be negative")
	}
	if limit < 1 || limit > 1000 {
		return nil, invalidInput("limit must be between 1 and 1000")
	}
	return s.Rows(ctx, `
SELECT u.ingest_sequence,u.event_id,u.status,u.error,u.manifest_id,u.topic,u.partition,u.offset,u.request_id,
 u.payment_session_id AS auth_id,s.allocation_id,s.app,s.orchestrator,a.pm_session_id,
 CAST(a.sequence_number AS INTEGER) AS sequence_number,a.num_tickets,
 CASE WHEN u.computed_fee_wei<>'' AND u.computed_fee_wei NOT GLOB '*[^0-9]*' AND (u.computed_fee_wei='0' OR substr(u.computed_fee_wei,1,1)<>'0') THEN u.computed_fee_wei END AS computed_fee_wei,
 u.computed_fee_usd,u.billable_seconds AS billable_secs,u.pixels,
 u.started_at_ms AS previous_time_unix,u.ended_at_ms AS current_time_unix,u.ended_at_ms AS signed_at_ms
FROM usage_events u
 LEFT JOIN signing_authorizations a ON a.usage_event_id=u.id
 LEFT JOIN payment_sessions s ON s.id=u.payment_session_id
WHERE u.ingest_sequence>? ORDER BY u.ingest_sequence LIMIT ?`, after, limit)
}

// ManifestCost sums the applied events of one manifest. A manifest belongs to one payment session.
func (s *Store) ManifestCost(ctx context.Context, manifest string) (map[string]any, error) {
	rows, err := s.DB.QueryContext(ctx, `
SELECT s.allocation_id,s.api_key_id,u.payment_session_id,s.app,s.orchestrator,a.pm_session_id,
 a.num_tickets,a.computed_fee_wei,a.computed_fee_usd,u.billable_seconds,a.sequence_number,a.signed_at_ms
FROM usage_events u
 JOIN signing_authorizations a ON a.usage_event_id=u.id
 JOIN payment_sessions s ON s.id=u.payment_session_id
WHERE u.manifest_id=? AND u.status='applied' ORDER BY u.ingest_sequence`, manifest)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]any{"manifest_id": manifest}
	feeWei, feeUSD, secs := new(big.Int), new(big.Int), new(big.Rat)
	var events, tickets int
	usdComplete := true
	var firstSequence, lastSequence, firstSigned, lastSigned int64
	for rows.Next() {
		var allocation, key, session, app, orch, pm, wei, sequence, billable string
		var usd sql.NullString
		var n int
		var signed int64
		if err := rows.Scan(&allocation, &key, &session, &app, &orch, &pm, &n, &wei, &usd, &billable, &sequence, &signed); err != nil {
			return nil, err
		}
		if events == 0 {
			out["allocation_id"], out["api_key_id"], out["payment_session_id"] = allocation, key, session
			out["app"], out["orchestrator"], out["pm_session_id"] = app, orch, pm
			firstSigned, lastSigned = signed, signed
		} else if session != out["payment_session_id"] {
			return nil, stateConflict("manifest spans payment sessions")
		}
		events++
		tickets += n
		amount, err := Amount(wei)
		if err != nil {
			return nil, fmt.Errorf("signing authorization fee: %w", err)
		}
		feeWei.Add(feeWei, amount)
		if !usd.Valid {
			usdComplete = false
		} else if usdComplete {
			raw, err := units.DecimalToUnits(usd.String, "USD")
			if err != nil {
				return nil, fmt.Errorf("signing authorization USD fee: %w", err)
			}
			amount, _ := Amount(raw)
			feeUSD.Add(feeUSD, amount)
		}
		seconds, ok := new(big.Rat).SetString(billable)
		if !ok {
			return nil, errors.New("invalid billable seconds")
		}
		secs.Add(secs, seconds)
		seq, err := strconv.ParseInt(sequence, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("signing authorization sequence: %w", err)
		}
		if events == 1 {
			firstSequence, lastSequence = seq, seq
		}
		firstSequence, lastSequence = min(firstSequence, seq), max(lastSequence, seq)
		firstSigned, lastSigned = min(firstSigned, signed), max(lastSigned, signed)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if events == 0 {
		return nil, sql.ErrNoRows
	}
	usd := ""
	if usdComplete {
		if usd, err = units.UnitsToDecimal(feeUSD.String()); err != nil {
			return nil, err
		}
	}
	out["event_count"], out["ticket_count"] = events, tickets
	out["fee_wei"], out["fee_usd"], out["billable_secs"] = feeWei.String(), usd, secs.FloatString(3)
	out["sequence_first"], out["sequence_last"] = firstSequence, lastSequence
	out["first_signed_at_ms"], out["last_signed_at_ms"] = firstSigned, lastSigned
	return out, nil
}
