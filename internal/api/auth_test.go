package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gatellm/internal/provider"
	"gatellm/internal/store"
)

const (
	testAdminToken = "admin-secret"
	testAPIKey     = "gk_test_key"
	testTenantID   = "00000000-0000-0000-0000-000000000001"
)

// fakeStore keeps tenants in memory, so these tests need no database.
type fakeStore struct {
	tenants map[string]*store.Tenant // tenant id -> tenant
	keys    map[string]string        // key hash -> tenant id
}

func newFakeStore() *fakeStore {
	fs := &fakeStore{tenants: map[string]*store.Tenant{}, keys: map[string]string{}}
	// One tenant that already exists, with a known key.
	fs.tenants[testTenantID] = &store.Tenant{ID: testTenantID, Name: "test-tenant", RPMLimit: 60, TPMLimit: 20000, MonthlyBudgetUSD: 5}
	fs.keys[hashKey(testAPIKey)] = testTenantID
	return fs
}

func (f *fakeStore) CreateTenant(ctx context.Context, name string, rpm, tpm int, budget float64, keyHash string) (*store.Tenant, error) {
	id := fmt.Sprintf("00000000-0000-0000-0000-%012d", len(f.tenants)+1)
	t := &store.Tenant{ID: id, Name: name, RPMLimit: rpm, TPMLimit: tpm, MonthlyBudgetUSD: budget, CreatedAt: time.Now()}
	f.tenants[id] = t
	f.keys[keyHash] = id
	return t, nil
}

func (f *fakeStore) AddKey(ctx context.Context, tenantID, keyHash string) error {
	if _, ok := f.tenants[tenantID]; !ok {
		return store.ErrNotFound
	}
	f.keys[keyHash] = tenantID
	return nil
}

func (f *fakeStore) FindByKeyHash(ctx context.Context, keyHash string) (*store.Tenant, error) {
	id, ok := f.keys[keyHash]
	if !ok {
		return nil, store.ErrNotFound
	}
	return f.tenants[id], nil
}

// newTestServer builds the real router with the fake store and only the mock provider.
func newTestServer(adminToken string) (http.Handler, *fakeStore) {
	fs := newFakeStore()
	h := &Handler{
		Providers:  provider.Registry{"mock": provider.NewMock(0, 0)},
		Tenants:    fs,
		AdminToken: adminToken,
	}
	return NewRouter(h, 1024), fs // 1 KB body limit, to test the size check
}

// do sends one request. authHeader is the full Authorization header ("" = none).
func do(handler http.Handler, method, path, body, authHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestChatNeedsValidAPIKey(t *testing.T) {
	srv, _ := newTestServer(testAdminToken)
	body := `{"model":"mock","messages":[{"role":"user","content":"hi"}]}`

	tests := []struct {
		name   string
		header string
		want   int
	}{
		{"no header", "", 401},
		{"wrong key", "Bearer gk_wrong", 401},
		{"not a bearer header", "Basic abc", 401},
		{"empty bearer", "Bearer ", 401},
		{"good key", "Bearer " + testAPIKey, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(srv, http.MethodPost, "/v1/chat/completions", body, tc.header)
			if rec.Code != tc.want {
				t.Errorf("got status %d, want %d, body %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestAuthPutsTenantInContext(t *testing.T) {
	h := &Handler{Tenants: newFakeStore()}
	var seen *store.Tenant
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = TenantFrom(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	h.Auth(next).ServeHTTP(httptest.NewRecorder(), req)

	if seen == nil || seen.Name != "test-tenant" {
		t.Errorf("tenant not found in context: %+v", seen)
	}
}

func TestAdminNeedsToken(t *testing.T) {
	body := `{"name":"acme"}`

	tests := []struct {
		name       string
		adminToken string // what the server was started with
		header     string
		want       int
	}{
		{"no header", testAdminToken, "", 401},
		{"wrong token", testAdminToken, "Bearer nope", 401},
		{"client key is not an admin token", testAdminToken, "Bearer " + testAPIKey, 401},
		{"right token", testAdminToken, "Bearer " + testAdminToken, 201},
		{"admin disabled, empty token sent", "", "Bearer ", 503},
		{"admin disabled, any token", "", "Bearer something", 503},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newTestServer(tc.adminToken)
			rec := do(srv, http.MethodPost, "/admin/tenants", body, tc.header)
			if rec.Code != tc.want {
				t.Errorf("got status %d, want %d, body %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestCreateTenantEndToEnd(t *testing.T) {
	srv, fs := newTestServer(testAdminToken)
	admin := "Bearer " + testAdminToken

	// 1. Create a tenant. We leave the limits out, so the defaults must be used.
	rec := do(srv, http.MethodPost, "/admin/tenants", `{"name":"acme"}`, admin)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Error("response with a secret should have Cache-Control: no-store")
	}

	var out struct {
		Tenant struct {
			ID       string  `json:"id"`
			Name     string  `json:"name"`
			RPMLimit int     `json:"rpm_limit"`
			TPMLimit int     `json:"tpm_limit"`
			Budget   float64 `json:"monthly_budget_usd"`
		} `json:"tenant"`
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Tenant.Name != "acme" || out.Tenant.RPMLimit != 60 || out.Tenant.TPMLimit != 20000 || out.Tenant.Budget != 5 {
		t.Errorf("wrong tenant or defaults: %+v", out.Tenant)
	}
	if !strings.HasPrefix(out.APIKey, "gk_") {
		t.Errorf("api key should start with gk_, got %q", out.APIKey)
	}

	// 2. Only the hash is stored, never the raw key.
	if _, stored := fs.keys[out.APIKey]; stored {
		t.Error("the raw key was stored")
	}
	if _, stored := fs.keys[hashKey(out.APIKey)]; !stored {
		t.Error("the key hash was not stored")
	}

	// 3. The new key works on the chat endpoint.
	chat := do(srv, http.MethodPost, "/v1/chat/completions",
		`{"model":"mock","messages":[{"role":"user","content":"hi"}]}`, "Bearer "+out.APIKey)
	if chat.Code != http.StatusOK {
		t.Errorf("new key should work, got %d: %s", chat.Code, chat.Body.String())
	}
}

func TestCreateTenantValidation(t *testing.T) {
	srv, _ := newTestServer(testAdminToken)
	admin := "Bearer " + testAdminToken

	tests := []struct {
		name string
		body string
	}{
		{"invalid json", `{oops`},
		{"missing name", `{}`},
		{"blank name", `{"name":"   "}`},
		{"rpm zero", `{"name":"a","rpm_limit":0}`},
		{"tpm negative", `{"name":"a","tpm_limit":-5}`},
		{"budget negative", `{"name":"a","monthly_budget_usd":-1}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(srv, http.MethodPost, "/admin/tenants", tc.body, admin)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("got status %d, want 400, body %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestAddTenantKey(t *testing.T) {
	srv, fs := newTestServer(testAdminToken)
	admin := "Bearer " + testAdminToken

	// Good tenant id: 201 and the new key works.
	rec := do(srv, http.MethodPost, "/admin/tenants/"+testTenantID+"/keys", "", admin)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
	var out struct {
		APIKey string `json:"api_key"`
	}
	json.Unmarshal(rec.Body.Bytes(), &out)
	if fs.keys[hashKey(out.APIKey)] != testTenantID {
		t.Error("new key hash was not saved for the tenant")
	}

	// Valid UUID but no such tenant: 404.
	rec = do(srv, http.MethodPost, "/admin/tenants/99999999-9999-9999-9999-999999999999/keys", "", admin)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown tenant: got %d, want 404", rec.Code)
	}

	// Not a UUID at all: 400.
	rec = do(srv, http.MethodPost, "/admin/tenants/not-a-uuid/keys", "", admin)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad id: got %d, want 400", rec.Code)
	}
}

func TestGenerateKey(t *testing.T) {
	raw1, hash1, err := generateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw2, _, _ := generateKey()

	if !strings.HasPrefix(raw1, "gk_") || len(raw1) != 3+64 {
		t.Errorf("unexpected key format: %q", raw1)
	}
	if raw1 == raw2 {
		t.Error("two generated keys must be different")
	}
	if hash1 == raw1 || hash1 != hashKey(raw1) {
		t.Error("hash must differ from the key and be repeatable")
	}
}
