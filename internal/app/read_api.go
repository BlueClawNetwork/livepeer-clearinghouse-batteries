package app

import (
	"context"
	"net/http"
	"strconv"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
)

const readAPIDefaultLimit = 200

// readAPIHandler serves cost reads for applications that pay through the signer.
// Callers present a management credential allowing cost.read in the
// Livepeer-Clearinghouse-Token header; responses follow the management API's
// conventions.
func readAPIHandler(ctx context.Context, db *store.Store, registry *serviceauth.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ctx.Err() != nil || db.DB.PingContext(r.Context()) != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	authorized := func(fn http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if status := registry.Check(r, "management", "cost.read"); status != http.StatusOK {
				message := "invalid read credential"
				if status == http.StatusForbidden {
					message = "read credential lacks cost.read"
				}
				managementErrorResponse(w, managementRequestError{status, message})
				return
			}
			fn(w, r)
		}
	}
	mux.HandleFunc("GET /v1/cost/events", authorized(func(w http.ResponseWriter, r *http.Request) {
		result, err := costEventsPage(r, db)
		managementResult(w, http.StatusOK, result, err)
	}))
	mux.HandleFunc("GET /v1/cost/manifests/{manifest_id}", authorized(func(w http.ResponseWriter, r *http.Request) {
		result, err := db.ManifestCost(r.Context(), r.PathValue("manifest_id"))
		managementResult(w, http.StatusOK, result, err)
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func readAPIQuery(r *http.Request, name string, fallback int64) (int64, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0, badManagementRequest("invalid " + name + ": expected a non-negative integer")
	}
	return n, nil
}

// costEventsPage is one page of the cost feed: events after the cursor, in
// ingest order, and the cursor to continue from.
func costEventsPage(r *http.Request, db *store.Store) (any, error) {
	after, err := readAPIQuery(r, "after", 0)
	if err != nil {
		return nil, err
	}
	limit, err := readAPIQuery(r, "limit", readAPIDefaultLimit)
	if err != nil {
		return nil, err
	}
	events, err := db.CostEvents(r.Context(), after, int(limit))
	if err != nil {
		return nil, err
	}
	next := after
	if len(events) > 0 {
		if last, ok := events[len(events)-1]["ingest_sequence"].(int64); ok {
			next = last
		}
	}
	return map[string]any{"events": events, "next_cursor": next}, nil
}
