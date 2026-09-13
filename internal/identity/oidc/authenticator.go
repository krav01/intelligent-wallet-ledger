// Package oidc verifies OIDC ID tokens and derives trusted principals.
package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
)

var (
	// ErrInvalidConfig indicates that an authenticator cannot safely verify tokens.
	ErrInvalidConfig = errors.New("oidc authenticator: invalid configuration")
	// ErrInvalidToken indicates that a token could not yield a trusted principal.
	ErrInvalidToken = errors.New("oidc token is invalid")
)

// Config identifies the issuer and token claims trusted by an Authenticator.
type Config struct {
	Issuer    string
	Audience  string
	RoleClaim string
}

// Principal is the verified subject and roles derived from an OIDC token.
type Principal struct {
	Subject string
	Roles   []string
}

// Authenticator verifies OIDC tokens from one configured issuer.
type Authenticator struct {
	verifier  *gooidc.IDTokenVerifier
	roleClaim string
}

// New discovers an OIDC provider and builds a verifier for its configured audience.
func New(ctx context.Context, config Config) (*Authenticator, error) {
	issuer, audience, roleClaim := strings.TrimSpace(config.Issuer), strings.TrimSpace(config.Audience), strings.TrimSpace(config.RoleClaim)
	if issuer == "" || audience == "" || roleClaim == "" {
		return nil, ErrInvalidConfig
	}
	provider, err := gooidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("discovering OIDC provider: %w", err)
	}
	return &Authenticator{verifier: provider.Verifier(&gooidc.Config{ClientID: audience}), roleClaim: roleClaim}, nil
}

// Authenticate verifies rawToken and returns its trusted subject and roles.
func (a *Authenticator) Authenticate(ctx context.Context, rawToken string) (Principal, error) {
	if a == nil || a.verifier == nil {
		return Principal{}, ErrInvalidConfig
	}
	if strings.TrimSpace(rawToken) == "" {
		return Principal{}, fmt.Errorf("%w: token is required", ErrInvalidToken)
	}
	token, err := a.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: verifying token: %w", ErrInvalidToken, err)
	}
	var claims map[string]json.RawMessage
	if err := token.Claims(&claims); err != nil {
		return Principal{}, fmt.Errorf("%w: decoding claims: %w", ErrInvalidToken, err)
	}
	var subject string
	if err := json.Unmarshal(claims["sub"], &subject); err != nil || strings.TrimSpace(subject) == "" {
		return Principal{}, fmt.Errorf("%w: subject is required", ErrInvalidToken)
	}
	var roles []string
	if err := json.Unmarshal(claims[a.roleClaim], &roles); err != nil {
		return Principal{}, fmt.Errorf("%w: roles claim is invalid", ErrInvalidToken)
	}
	return Principal{Subject: subject, Roles: roles}, nil
}
