package agentmcp

import (
	"errors"
	"flag"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/xerrors"

	"github.com/coder/safedial"
)

// requestOrigin is a lowercased URL scheme, hostname, and port, with the
// port defaulted from the scheme when absent.
type requestOrigin struct {
	scheme string
	host   string
	port   string
}

func originOf(u *url.URL) requestOrigin {
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	return requestOrigin{scheme: scheme, host: strings.ToLower(u.Hostname()), port: port}
}

func (o requestOrigin) String() string {
	return o.scheme + "://" + net.JoinHostPort(o.host, o.port)
}

// originPinnedHTTPClient pins configured headers and redirects to the
// scheme, host, and port of serverURL. Headers are sent only to that
// origin and never replace a header the caller already set; with headers
// configured, a request to any other origin fails. Redirects that leave
// the origin are refused whether or not headers are configured.
func originPinnedHTTPClient(serverURL string, headers map[string]string) (*http.Client, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		// The *url.Error repeats the raw URL, which can carry credentials.
		if urlErr, ok := errors.AsType[*url.Error](err); ok {
			err = urlErr.Err
		}
		return nil, xerrors.Errorf("invalid server url: %w", err)
	}
	origin := originOf(u)

	base := http.DefaultTransport
	if isolated := mcpHTTPClient(); isolated != nil {
		base = isolated.Transport
	}
	if len(headers) > 0 {
		base = &headerRoundTripper{
			base:    base,
			origin:  origin,
			headers: headers,
		}
	}
	return &http.Client{
		Transport:     base,
		CheckRedirect: sameOriginRedirectPolicy(origin),
	}, nil
}

// sameOriginRedirectPolicy checks the origin before safedial does so
// that the refusal names the fix. Setting CheckRedirect replaces
// net/http's default 10-hop limit, which safedial enforces.
func sameOriginRedirectPolicy(origin requestOrigin) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if target := originOf(req.URL); target != origin {
			return xerrors.Errorf("refusing redirect from %s to %s; set the server url in the MCP config to the redirect target",
				origin, target)
		}
		return safedial.CheckSameOriginRedirect(req, via)
	}
}

type headerRoundTripper struct {
	base    http.RoundTripper
	origin  requestOrigin
	headers map[string]string
	// refused holds the first off-origin refusal.
	refused atomic.Pointer[error]
}

func (h *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if target := originOf(req.URL); target != h.origin {
		err := xerrors.Errorf("refusing request to %s: configured headers are pinned to %s and the MCP server directed the client off it; use a server whose message endpoint is on %s or remove the headers",
			target, h.origin, h.origin)
		h.refused.CompareAndSwap(nil, &err)
		return nil, err
	}
	clone := req.Clone(req.Context())
	for k, v := range h.headers {
		if len(clone.Header.Values(k)) > 0 {
			continue
		}
		clone.Header.Set(k, v)
	}
	return h.base.RoundTrip(clone)
}

// headerRefusal returns the first request that tr's HTTP client refused
// for leaving the origin its headers are pinned to, or nil.
func headerRefusal(tr mcp.Transport) error {
	var client *http.Client
	switch t := tr.(type) {
	case *mcp.StreamableClientTransport:
		client = t.HTTPClient
	case *mcp.SSEClientTransport:
		client = t.HTTPClient
	}
	if client == nil {
		return nil
	}
	h, ok := client.Transport.(*headerRoundTripper)
	if !ok {
		return nil
	}
	if err := h.refused.Load(); err != nil {
		return *err
	}
	return nil
}

// errorURLPattern matches an absolute URL as it appears in an error
// message, quoted or not.
var errorURLPattern = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s"'<>]*`)

// redactErrorURLs returns err's message with every absolute URL reduced
// to scheme and host, and every relative URL of a *url.Error in err's
// tree replaced, since URL userinfo, paths, and queries can carry
// credentials.
func redactErrorURLs(err error) string {
	msg := err.Error()
	// url.Error quotes its URL, and a refused redirect reports the raw
	// Location, which can be a bare path.
	for _, ref := range relativeErrorURLs(err) {
		msg = strings.ReplaceAll(msg, strconv.Quote(ref), `"<relative url>"`)
	}
	return errorURLPattern.ReplaceAllStringFunc(msg, func(raw string) string {
		scheme, rest, _ := strings.Cut(raw, "://")
		authority := rest
		if i := strings.IndexAny(rest, "/?#"); i >= 0 {
			authority = rest[:i]
		}
		if i := strings.LastIndex(authority, "@"); i >= 0 {
			authority = authority[i+1:]
		}
		return scheme + "://" + authority
	})
}

func relativeErrorURLs(err error) []string {
	var refs []string
	var walk func(error)
	walk = func(err error) {
		if err == nil {
			return
		}
		if urlErr, ok := err.(*url.Error); ok { //nolint:errorlint // Walks the tree itself.
			if u, perr := url.Parse(urlErr.URL); urlErr.URL != "" && (perr != nil || u.Scheme == "" && u.Host == "") {
				refs = append(refs, urlErr.URL)
			}
		}
		switch e := err.(type) { //nolint:errorlint // Walks the tree itself.
		case interface{ Unwrap() error }:
			walk(e.Unwrap())
		case interface{ Unwrap() []error }:
			for _, inner := range e.Unwrap() {
				walk(inner)
			}
		}
	}
	walk(err)
	return refs
}

// mcpHTTPClient returns an isolated *http.Client when running
// inside tests, or nil for production. During tests,
// httptest.Server.Close() calls
// http.DefaultTransport.CloseIdleConnections(), which disrupts
// any MCP client sharing that transport. When DefaultTransport
// is a *http.Transport it is cloned; otherwise a minimal
// transport with ProxyFromEnvironment is created as a fallback.
func mcpHTTPClient() *http.Client {
	if flag.Lookup("test.v") == nil {
		return nil
	}
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		return &http.Client{Transport: dt.Clone()}
	}
	return &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
	}}
}
