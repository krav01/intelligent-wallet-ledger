// Package reviewdecision exposes authenticated analyst review decisions over HTTP.
package reviewdecision

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/krav01/intelligent-wallet-ledger/internal/app/reviewcase"
	"github.com/krav01/intelligent-wallet-ledger/internal/identity/oidc"
	transferpostgres "github.com/krav01/intelligent-wallet-ledger/internal/transfer/postgres"
)

const maxBodyBytes = 4 << 10

// ErrInvalidDependency indicates that a handler dependency is missing.
var ErrInvalidDependency = errors.New("review decision HTTP handler: invalid dependency")

type authenticator interface {
	Authenticate(context.Context, string) (oidc.Principal, error)
}

type decisionHandler interface {
	Decide(context.Context, reviewcase.TrustedPrincipal, string, reviewcase.Decision) error
}

// Handler authenticates analyst commands and delegates them to the review-case application service.
type Handler struct {
	authenticator authenticator
	decider       decisionHandler
	logger        *slog.Logger
}

// New creates a handler that rejects all requests unless they contain a verified analyst token.
func New(authenticator authenticator, decider decisionHandler, logger *slog.Logger) (*Handler, error) {
	if authenticator == nil || decider == nil || logger == nil {
		return nil, ErrInvalidDependency
	}
	return &Handler{authenticator: authenticator, decider: decider, logger: logger}, nil
}

// RegisterRoutes adds the analyst decision endpoint to a server mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("POST /v1/transfers/{transferID}/review-decision", h)
}

// ServeHTTP processes POST /v1/transfers/{transferID}/review-decision requests.
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	rawToken, ok := bearerToken(request.Header.Get("Authorization"))
	if !ok {
		writeError(writer, http.StatusUnauthorized, "authentication is required")
		return
	}
	principal, err := h.authenticator.Authenticate(request.Context(), rawToken)
	if err != nil {
		if errors.Is(err, oidc.ErrInvalidToken) {
			writeError(writer, http.StatusUnauthorized, "authentication failed")
			return
		}
		h.logger.ErrorContext(request.Context(), "review decision authentication failed", "error", err)
		writeError(writer, http.StatusInternalServerError, "internal server error")
		return
	}
	if !hasRole(principal.Roles, reviewcase.RoleAnalyst) {
		writeError(writer, http.StatusForbidden, "analyst role is required")
		return
	}
	decision, err := decodeDecision(writer, request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid request body")
		return
	}
	err = h.decider.Decide(request.Context(), reviewcase.TrustedPrincipal{
		Subject: principal.Subject,
		Role:    reviewcase.RoleAnalyst,
	}, request.PathValue("transferID"), decision)
	if err != nil {
		h.writeDecisionError(writer, request, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeDecisionError(writer http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, reviewcase.ErrInvalidArgument):
		writeError(writer, http.StatusBadRequest, "invalid review decision")
	case errors.Is(err, reviewcase.ErrUnauthorized):
		writeError(writer, http.StatusForbidden, "analyst role is required")
	case errors.Is(err, transferpostgres.ErrNotFound):
		writeError(writer, http.StatusNotFound, "transfer not found")
	case errors.Is(err, transferpostgres.ErrStateConflict):
		writeError(writer, http.StatusConflict, "review case is no longer open")
	default:
		h.logger.ErrorContext(request.Context(), "review decision failed", "error", err)
		writeError(writer, http.StatusInternalServerError, "internal server error")
	}
}

func bearerToken(value string) (string, bool) {
	parts := strings.Fields(value)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	return parts[1], true
}

func hasRole(roles []string, target reviewcase.Role) bool {
	return slices.Contains(roles, string(target))
}

func decodeDecision(writer http.ResponseWriter, request *http.Request) (reviewcase.Decision, error) {
	defer func() { _ = request.Body.Close() }()
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	var body struct {
		Decision reviewcase.Decision `json:"decision"`
	}
	if err := decoder.Decode(&body); err != nil {
		return "", err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", errors.New("request body must contain one JSON object")
	}
	return body.Decision, nil
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": message})
}
