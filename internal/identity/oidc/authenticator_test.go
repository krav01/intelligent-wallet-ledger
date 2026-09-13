package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
)

func TestNewRejectsIncompleteConfig(t *testing.T) {
	for _, test := range []struct {
		name   string
		config Config
	}{
		{name: "missing issuer", config: Config{Audience: "wallet-api", RoleClaim: "roles"}},
		{name: "missing audience", config: Config{Issuer: "https://issuer.example", RoleClaim: "roles"}},
		{name: "missing role claim", config: Config{Issuer: "https://issuer.example", Audience: "wallet-api"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(t.Context(), test.config)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestAuthenticator_Authenticate(t *testing.T) {
	const (
		issuer   = "https://issuer.example"
		audience = "wallet-api"
	)

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := jose.JSONWebKey{
		Key:       &privateKey.PublicKey,
		KeyID:     "test-key",
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}
	client := &http.Client{Transport: discoveryTransport(t, issuer, publicKey)}
	ctx := gooidc.ClientContext(context.Background(), client)
	authenticator, err := New(ctx, Config{Issuer: issuer, Audience: audience, RoleClaim: "roles"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for _, test := range []struct {
		name      string
		claims    map[string]any
		wantErr   error
		principal Principal
	}{
		{
			name: "verified principal",
			claims: map[string]any{
				"sub":   "analyst-42",
				"roles": []string{"analyst"},
			},
			principal: Principal{Subject: "analyst-42", Roles: []string{"analyst"}},
		},
		{
			name: "missing subject",
			claims: map[string]any{
				"roles": []string{"analyst"},
			},
			wantErr: ErrInvalidToken,
		},
		{
			name: "malformed roles",
			claims: map[string]any{
				"sub":   "analyst-42",
				"roles": "analyst",
			},
			wantErr: ErrInvalidToken,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rawToken := signedToken(t, privateKey, issuer, audience, test.claims)
			principal, err := authenticator.Authenticate(t.Context(), rawToken)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Authenticate() error = %v, want %v", err, test.wantErr)
			}
			if principal.Subject != test.principal.Subject || !sameStrings(principal.Roles, test.principal.Roles) {
				t.Errorf("Authenticate() principal = %+v, want %+v", principal, test.principal)
			}
		})
	}
}

func signedToken(t *testing.T, privateKey *rsa.PrivateKey, issuer, audience string, claims map[string]any) string {
	t.Helper()
	claims["iss"] = issuer
	claims["aud"] = audience
	claims["exp"] = time.Now().Add(time.Minute).Unix()

	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: privateKey}, nil)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	rawToken, err := compact.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return rawToken
}

func discoveryTransport(t *testing.T, issuer string, key jose.JSONWebKey) http.RoundTripper {
	t.Helper()
	return roundTripFunc(func(request *http.Request) (*http.Response, error) {
		var body any
		switch request.URL.String() {
		case issuer + "/.well-known/openid-configuration":
			body = map[string]string{"issuer": issuer, "jwks_uri": issuer + "/keys"}
		case issuer + "/keys":
			body = map[string][]jose.JSONWebKey{"keys": {key}}
		default:
			t.Errorf("unexpected OIDC request: %s", request.URL)
			return nil, errors.New("unexpected oidc request")
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(encoded))),
			Header:     make(http.Header),
			Request:    request,
		}, nil
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
