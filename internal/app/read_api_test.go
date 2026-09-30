package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

const readToken = "read-token"

// readRegistry has one application credential allowing cost.read, one management
// credential without it, and the webhook credential.
func readRegistry(t *testing.T) *serviceauth.Registry {
	t.Helper()
	return testRegistry(t,
		`{"id":"app","secret":"read-token","management":{"allow":["cost.read"]}}`,
		`{"id":"reports","secret":"reports-token","management":{"allow":["usage.read"]}}`,
		`{"id":"signer","secret":"test-webhook-secret","webhook":{"authorize":true}}`)
}

func readRequest(t *testing.T, handler http.Handler, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("GET", path, nil)
	if token != "" {
		r.Header.Set("Livepeer-Clearinghouse-Token", token)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestReadAPIRoutes(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	handler := readAPIHandler(ctx, f.DB, readRegistry(t))
	for i := range 3 {
		require.NoError(t, f.DB.Ingest(ctx, "events", 0, int64(i), f.EventWith(t, fmt.Sprint("m1-", i), "1000000000000000", testutil.PM, map[string]any{"manifest_id": "m-1", "sequence_number": i, "computed_fee_usd": "0.001"})))
	}
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 3, f.EventWith(t, "m1-bad", "1", testutil.PM, map[string]any{"manifest_id": "m-1", "session_id": "state-other"})))
	require.NoError(t, f.DB.Ingest(ctx, "events", 0, 4, f.EventWith(t, "m2", "5", testutil.PM, map[string]any{"manifest_id": "m-2"})))

	manifest := managementObject(t, readRequest(t, handler, "/v1/cost/manifests/m-1", readToken), 200)
	require.Equal(t, "0.003", manifest["fee_eth"])
	require.Equal(t, "0.003", manifest["fee_usd"])
	require.NotContains(t, manifest, "fee_wei")
	require.Equal(t, float64(3), manifest["event_count"])
	require.Equal(t, float64(2), manifest["sequence_last"])
	require.Equal(t, f.Allocation, manifest["allocation_id"])
	managementObject(t, readRequest(t, handler, "/v1/cost/manifests/unknown", readToken), 404)

	var seen []string
	after := 0
	for range 3 {
		page := managementObject(t, readRequest(t, handler, fmt.Sprintf("/v1/cost/events?after=%d&limit=2", after), readToken), 200)
		events := page["events"].([]any)
		for _, item := range events {
			seen = append(seen, item.(map[string]any)["event_id"].(string))
		}
		after = int(page["next_cursor"].(float64))
	}
	require.Equal(t, []string{"m1-0", "m1-1", "m1-2", "m1-bad", "m2"}, seen)
	require.Equal(t, 5, after)
	empty := managementObject(t, readRequest(t, handler, "/v1/cost/events?after=5", readToken), 200)
	require.Empty(t, empty["events"])
	require.Equal(t, float64(5), empty["next_cursor"])

	page := managementObject(t, readRequest(t, handler, "/v1/cost/events?after=3&limit=1", readToken), 200)
	quarantined := page["events"].([]any)[0].(map[string]any)
	require.Equal(t, "quarantined", quarantined["status"])
	require.Equal(t, "event does not match session binding", quarantined["error"])
	require.Nil(t, quarantined["allocation_id"])
	require.Nil(t, quarantined["sequence_number"])
	require.Equal(t, "0.000000000000000001", quarantined["computed_fee_eth"])
	applied := managementObject(t, readRequest(t, handler, "/v1/cost/events?limit=1", readToken), 200)["events"].([]any)[0].(map[string]any)
	require.Equal(t, "0.001", applied["computed_fee_eth"])
	require.Equal(t, "0.001", applied["computed_fee_usd"])
	require.Equal(t, float64(1), applied["ingest_sequence"])
	require.NotContains(t, applied, "computed_fee_wei")
	body, err := json.Marshal(applied)
	require.NoError(t, err)
	for _, key := range []string{"ingest_sequence", "event_id", "status", "error", "manifest_id", "topic", "partition", "offset", "request_id", "auth_id", "allocation_id", "app", "orchestrator", "pm_session_id", "sequence_number", "num_tickets", "computed_fee_eth", "computed_fee_usd", "billable_secs", "pixels", "previous_time_unix", "current_time_unix", "signed_at_ms"} {
		require.Contains(t, string(body), `"`+key+`"`)
	}

	for _, path := range []string{"/v1/cost/events?after=-1", "/v1/cost/events?after=x", "/v1/cost/events?limit=0", "/v1/cost/events?limit=1001"} {
		managementObject(t, readRequest(t, handler, path, readToken), 400)
	}
	for _, token := range []string{"", "wrong", "test-webhook-secret"} {
		require.Equal(t, "invalid read credential", managementObject(t, readRequest(t, handler, "/v1/cost/events", token), 401)["error"])
		managementObject(t, readRequest(t, handler, "/v1/cost/manifests/m-1", token), 401)
	}
	require.Equal(t, "read credential lacks cost.read", managementObject(t, readRequest(t, handler, "/v1/cost/events", "reports-token"), 403)["error"])
	require.Equal(t, 401, readRequest(t, readAPIHandler(ctx, f.DB, nil), "/v1/cost/events", readToken).Code, "no registry, no access")
	r := httptest.NewRequest("POST", "/v1/cost/events", nil)
	r.Header.Set("Livepeer-Clearinghouse-Token", readToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	require.Equal(t, 405, w.Code)
	for _, path := range []string{"/livez", "/readyz"} {
		require.Equal(t, 200, readRequest(t, handler, path, "").Code)
	}
	require.Equal(t, 404, readRequest(t, handler, "/v1/grants", readToken).Code)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.Equal(t, 503, readRequest(t, readAPIHandler(cancelled, f.DB, readRegistry(t)), "/readyz", "").Code)
}

func TestReadAPIValidation(t *testing.T) {
	for _, address := range []string{":8082", "127.0.0.2:8082", "[::1]:8082"} {
		require.NoError(t, (ServeParams{EnableReadAPI: mustHTTPBind(t, address), CredsFile: "creds.json"}).Validate())
	}
	for _, address := range []string{"0.0.0.0:8082", "10.0.0.1:8082"} {
		p := ServeParams{EnableReadAPI: mustHTTPBind(t, address), CredsFile: "creds.json"}
		require.ErrorContains(t, p.Validate(), "--unsafe-http-bind")
		p.UnsafeHTTPBind = true
		require.NoError(t, p.Validate())
	}
	require.ErrorContains(t, (ServeParams{EnableReadAPI: mustHTTPBind(t, ":8082")}).Validate(), "creds-file")
	p := ServeParams{EnableReadAPI: mustHTTPBind(t, ":8082"), EnableAuthWebhook: mustHTTPBind(t, "[::1]:8082"), CredsFile: "creds.json"}
	require.ErrorContains(t, p.Validate(), "separate TCP ports")
	p.EnableAuthWebhook = HTTPBind{}
	p.EnableManagementAPI = mustHTTPBind(t, ":8082")
	require.ErrorContains(t, p.Validate(), "separate TCP ports")
	p.EnableManagementAPI = mustHTTPBind(t, ":8081")
	require.NoError(t, p.Validate())
}

func TestReadAPIRequiresAReadCredential(t *testing.T) {
	// A credentials file with no management entry cannot serve the read API.
	p := ServeParams{Common: Common{DBPath: filepath.Join(t.TempDir(), "read.db")}, EnableReadAPI: mustHTTPBind(t, testutil.Port(t)),
		CredsFile: testCredsFile(t, `{"id":"signer","secret":"s","webhook":{"authorize":true}}`)}
	require.ErrorContains(t, Serve(context.Background(), p), "cost.read")
}

func TestReadAPIServerStartup(t *testing.T) {
	readBind, managementBind := testutil.Port(t), testutil.Port(t)
	for managementBind == readBind {
		managementBind = testutil.Port(t)
	}
	p := ServeParams{Common: Common{DBPath: filepath.Join(t.TempDir(), "read.db")}, EnableReadAPI: mustHTTPBind(t, readBind), EnableManagementAPI: mustHTTPBind(t, managementBind),
		CredsFile: testCredsFile(t, testHTTPCredentials, `{"id":"app","secret":"read-token","management":{"allow":["cost.read"]}}`)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, p) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Error("read API server shutdown timed out")
		}
	})
	client := &http.Client{Timeout: time.Second}
	base := "http://" + readBind
	testutil.Eventually(t, func() bool {
		response, err := client.Get(base + "/readyz")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == 200
	})
	request, err := http.NewRequest("GET", base+"/v1/cost/events", nil)
	require.NoError(t, err)
	request.Header.Set("Livepeer-Clearinghouse-Token", readToken)
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
	var page map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&page))
	require.Equal(t, float64(0), page["next_cursor"])
	response, err = client.Get("http://" + managementBind + "/v1/cost/events")
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, 404, response.StatusCode, "cost routes are not served by the management listener")
}
