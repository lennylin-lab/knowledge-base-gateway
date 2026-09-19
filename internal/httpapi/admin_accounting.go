package httpapi

// V1.4 cost-governance admin surface: pricing catalog and budget policy
// management plus the budget-utilization section of /admin/usage. Endpoints
// are disabled (404) unless an AccountingAdmin implementation is wired
// (database mode). Mutations commit atomically with their management-audit
// record (the mgmt.writeOp pattern) and record metadata only — never
// secrets, key material, or request content.

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/mgmt"
)

// AccountingAdmin is the management surface for pricing and budgets.
type AccountingAdmin interface {
	ListPrices(ctx context.Context) ([]mgmt.PriceView, error)
	UpsertPrice(ctx context.Context, in mgmt.PriceInput, op mgmt.AdminOp) error
	ListBudgets(ctx context.Context) ([]mgmt.BudgetView, error)
	UpsertBudget(ctx context.Context, in mgmt.BudgetInput, op mgmt.AdminOp) error
	BudgetUsage(ctx context.Context, now time.Time) ([]mgmt.BudgetUsageView, error)
}

// currencyPattern mirrors the schema's CHECK constraint: ISO-4217 style
// three-letter uppercase currency codes.
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// registerAccountingAdmin mounts /admin/prices and /admin/budgets. Called
// from NewAdminMux only when deps.Accounting is non-nil.
func registerAccountingAdmin(mux *http.ServeMux, guard func(http.HandlerFunc) http.HandlerFunc, deps AdminDeps) {
	mux.HandleFunc("/admin/prices", guard(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			rows, err := deps.Accounting.ListPrices(r.Context())
			if err != nil {
				writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query prices")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"prices": rows})
		case http.MethodPost:
			handleUpsertPrice(w, r, deps)
		default:
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST")
		}
	}))
	mux.HandleFunc("/admin/budgets", guard(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			rows, err := deps.Accounting.ListBudgets(r.Context())
			if err != nil {
				writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "query_failed", "could not query budgets")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"budgets": rows})
		case http.MethodPost:
			handleUpsertBudget(w, r, deps)
		default:
			writeError(w, newRequestID(), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET or POST")
		}
	}))
}

// priceBody is the price upsert payload. Micros are integers; the optional
// token classes may be null (unpriced: costs needing them stay unknown).
type priceBody struct {
	Provider                  string `json:"provider"`
	PublicModel               string `json:"public_model"`
	PriceVersion              int    `json:"price_version"`
	Currency                  string `json:"currency"`
	InputMicrosPerToken       int64  `json:"input_micros_per_token"`
	OutputMicrosPerToken      int64  `json:"output_micros_per_token"`
	ReasoningMicrosPerToken   *int64 `json:"reasoning_micros_per_token"`
	CachedInputMicrosPerToken *int64 `json:"cached_input_micros_per_token"`
	EffectiveFrom             string `json:"effective_from"` // RFC3339; empty means now
}

func handleUpsertPrice(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	var body priceBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil {
		writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_request", "invalid price payload")
		return
	}
	if body.Provider == "" || body.PublicModel == "" || body.PriceVersion <= 0 ||
		!currencyPattern.MatchString(body.Currency) ||
		body.InputMicrosPerToken < 0 || body.OutputMicrosPerToken < 0 ||
		(body.ReasoningMicrosPerToken != nil && *body.ReasoningMicrosPerToken < 0) ||
		(body.CachedInputMicrosPerToken != nil && *body.CachedInputMicrosPerToken < 0) {
		writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_request", "provider, public_model, positive price_version, currency, and non-negative micros are required")
		return
	}
	effectiveFrom := time.Now()
	if body.EffectiveFrom != "" {
		t, err := time.Parse(time.RFC3339, body.EffectiveFrom)
		if err != nil {
			writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_request", "effective_from must be RFC3339")
			return
		}
		effectiveFrom = t
	}
	in := mgmt.PriceInput{
		Provider: body.Provider, PublicModel: body.PublicModel,
		PriceVersion:              body.PriceVersion,
		Currency:                  body.Currency,
		InputMicrosPerToken:       body.InputMicrosPerToken,
		OutputMicrosPerToken:      body.OutputMicrosPerToken,
		ReasoningMicrosPerToken:   body.ReasoningMicrosPerToken,
		CachedInputMicrosPerToken: body.CachedInputMicrosPerToken,
		EffectiveFrom:             effectiveFrom,
	}
	detail, _ := json.Marshal(map[string]any{
		"provider": in.Provider, "public_model": in.PublicModel,
		"price_version": in.PriceVersion, "currency": in.Currency,
		"effective_from": in.EffectiveFrom,
	})
	op := mgmt.AdminOp{CreatedAt: time.Now(), Action: "price_upsert",
		Target: in.Provider + "/" + in.PublicModel + "/v" + strconv.Itoa(in.PriceVersion), Detail: detail}
	if err := deps.Accounting.UpsertPrice(r.Context(), in, op); err != nil {
		writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "update_failed", "could not upsert the price")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "upserted", "provider": in.Provider,
		"public_model": in.PublicModel, "price_version": in.PriceVersion})
}

// budgetBody is the budget upsert payload.
type budgetBody struct {
	Scope        string `json:"scope"` // "subject" or "tenant"
	SubjectID    string `json:"subject_id"`
	TenantID     string `json:"tenant_id"`
	Period       string `json:"period"` // "daily" or "monthly"
	Currency     string `json:"currency"`
	AmountMicros int64  `json:"amount_micros"`
	Enabled      *bool  `json:"enabled"` // nil defaults to true
}

func handleUpsertBudget(w http.ResponseWriter, r *http.Request, deps AdminDeps) {
	var body budgetBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil {
		writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_request", "invalid budget payload")
		return
	}
	if (body.Scope != "subject" && body.Scope != "tenant") ||
		(body.Scope == "subject" && body.SubjectID == "") ||
		(body.Scope == "tenant" && body.SubjectID != "") ||
		body.TenantID == "" ||
		(body.Period != "daily" && body.Period != "monthly") ||
		!currencyPattern.MatchString(body.Currency) || body.AmountMicros <= 0 {
		writeError(w, newRequestID(), http.StatusBadRequest, "invalid_request_error", "invalid_request", "scope (subject rows carry subject_id; tenant rows must not), tenant_id, period, currency, and a positive amount_micros are required")
		return
	}
	in := mgmt.BudgetInput{
		Scope: body.Scope, SubjectID: body.SubjectID, TenantID: body.TenantID,
		Period: body.Period, Currency: body.Currency,
		AmountMicros: body.AmountMicros, Enabled: body.Enabled,
	}
	target := in.TenantID
	if in.SubjectID != "" {
		target = in.SubjectID + "@" + in.TenantID
	}
	detail, _ := json.Marshal(map[string]any{
		"scope": in.Scope, "period": in.Period, "currency": in.Currency,
	})
	op := mgmt.AdminOp{CreatedAt: time.Now(), Action: "budget_upsert", Target: target, Detail: detail}
	if err := deps.Accounting.UpsertBudget(r.Context(), in, op); err != nil {
		writeError(w, newRequestID(), http.StatusInternalServerError, "internal_error", "update_failed", "could not upsert the budget")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "upserted", "scope": in.Scope, "period": in.Period})
}

// budgetUsageSection builds the /admin/usage "budgets" block. ok is false
// when no accounting admin is wired, leaving the section out (additive
// evolution).
func budgetUsageSection(deps AdminDeps, r *http.Request) (rows []mgmt.BudgetUsageView, ok bool) {
	if deps.Accounting == nil {
		return nil, false
	}
	rows, err := deps.Accounting.BudgetUsage(r.Context(), time.Now())
	if err != nil {
		return nil, true // wired but failed: caller answers query_failed
	}
	return rows, true
}
