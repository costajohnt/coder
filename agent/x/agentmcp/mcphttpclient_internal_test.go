package agentmcp

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"github.com/coder/coder/v2/testutil"
)

type headerRecorder struct {
	mu   sync.Mutex
	seen map[string][]http.Header
}

func (r *headerRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	if r.seen == nil {
		r.seen = make(map[string][]http.Header)
	}
	r.seen[req.URL.Path] = append(r.seen[req.URL.Path], req.Header.Clone())
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (r *headerRecorder) requests(path string) []http.Header {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]http.Header(nil), r.seen[path]...)
}

func TestOriginPinnedHTTPClient(t *testing.T) {
	t.Parallel()

	const (
		headerName  = "X-Mcp-Test"
		headerValue = "configured"
	)

	otherRec := &headerRecorder{}
	otherSrv := httptest.NewServer(otherRec)
	t.Cleanup(otherSrv.Close)

	originRec := &headerRecorder{}
	originSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/redirect" {
			http.Redirect(w, req, otherSrv.URL+"/landed", http.StatusFound)
			return
		}
		originRec.ServeHTTP(w, req)
	}))
	t.Cleanup(originSrv.Close)

	withHeaders, err := originPinnedHTTPClient(originSrv.URL+"/mcp", map[string]string{headerName: headerValue})
	require.NoError(t, err)
	noHeaders, err := originPinnedHTTPClient(originSrv.URL+"/mcp", nil)
	require.NoError(t, err)

	get := func(t *testing.T, c *http.Client, rawURL string, header http.Header) error {
		t.Helper()
		ctx := testutil.Context(t, testutil.WaitShort)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		require.NoError(t, err)
		for k, v := range header {
			req.Header[k] = v
		}
		resp, err := c.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		return err
	}

	t.Run("SameOriginGetsHeader", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, get(t, withHeaders, originSrv.URL+"/same", nil))

		seen := originRec.requests("/same")
		require.Len(t, seen, 1)
		assert.Equal(t, []string{headerValue}, seen[0].Values(headerName))
	})

	t.Run("ClientHeaderWins", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, get(t, withHeaders, originSrv.URL+"/client", http.Header{headerName: {"from-client"}}))

		seen := originRec.requests("/client")
		require.Len(t, seen, 1)
		assert.Equal(t, []string{"from-client"}, seen[0].Values(headerName))
	})

	t.Run("OtherOriginRefusedWithHeaders", func(t *testing.T) {
		t.Parallel()
		err := get(t, withHeaders, otherSrv.URL+"/direct", nil)
		require.ErrorContains(t, err, "configured headers are pinned to")
		assert.Empty(t, otherRec.requests("/direct"), "request must not reach another origin")
	})

	t.Run("OtherOriginAllowedWithoutHeaders", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, get(t, noHeaders, otherSrv.URL+"/direct-noheaders", nil))
		assert.Len(t, otherRec.requests("/direct-noheaders"), 1)
	})

	t.Run("CrossOriginRedirectRefused", func(t *testing.T) {
		t.Parallel()
		err := get(t, withHeaders, originSrv.URL+"/redirect", nil)
		require.ErrorContains(t, err, "refusing redirect")
		require.ErrorContains(t, err, "set the server url in the MCP config to the redirect target")
		assert.Empty(t, otherRec.requests("/landed"), "redirect target must not be requested")
	})

	t.Run("CrossOriginRedirectRefusedWithoutHeaders", func(t *testing.T) {
		t.Parallel()
		err := get(t, noHeaders, originSrv.URL+"/redirect", nil)
		require.ErrorContains(t, err, "refusing redirect")
	})
}

func TestSameOriginRedirectPolicy(t *testing.T) {
	t.Parallel()

	serverURL, err := url.Parse("https://mcp.example.com/mcp")
	require.NoError(t, err)
	policy := sameOriginRedirectPolicy(originOf(serverURL))

	newReq := func(t *testing.T, method, rawURL string) *http.Request {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, rawURL, nil)
		require.NoError(t, err)
		return req
	}

	tests := []struct {
		name    string
		method  string
		target  string
		wantErr string
	}{
		{name: "SameOrigin", target: "https://mcp.example.com/other/path"},
		{name: "SameOriginDifferentCase", target: "https://MCP.example.com/x"},
		{name: "ExplicitDefaultPort", target: "https://mcp.example.com:443/x"},
		{name: "HTTPSToHTTP", target: "http://mcp.example.com/x", wantErr: "refusing redirect"},
		{name: "OtherHost", target: "https://evil.example.com/x", wantErr: "refusing redirect"},
		{name: "OtherPort", target: "https://mcp.example.com:8443/x", wantErr: "refusing redirect"},
		{name: "MethodChange", method: http.MethodPost, target: "https://mcp.example.com/x", wantErr: "changed method"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := policy(newReq(t, http.MethodGet, tt.target), []*http.Request{newReq(t, tt.method, serverURL.String())})
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}

	t.Run("TooManyRedirects", func(t *testing.T) {
		t.Parallel()
		via := make([]*http.Request, 10)
		for i := range via {
			via[i] = newReq(t, http.MethodGet, serverURL.String())
		}
		assert.ErrorContains(t, policy(newReq(t, http.MethodGet, "https://mcp.example.com/x"), via), "stopped after")
	})
}

func TestOriginPinnedHTTPClient_InvalidURLOmitsURL(t *testing.T) {
	t.Parallel()

	_, err := originPinnedHTTPClient("http://user:pa55word@[::1/mcp?api_key=SECRETQ", nil)
	require.ErrorContains(t, err, "invalid server url")
	assert.NotContains(t, err.Error(), "pa55word")
	assert.NotContains(t, err.Error(), "SECRETQ")
}

func TestRedactErrorURLs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "URLErrorInTree",
			err: errors.Join(
				xerrors.Errorf("sending: %w", &url.Error{
					Op:  "Post",
					URL: "http://user:***@127.0.0.1:1/s/SECRETPATH/mcp?api_key=SECRETQ",
					Err: xerrors.New("connection refused"),
				}),
				xerrors.New("configured http://user:pa55word@127.0.0.1:1/s/SECRETPATH/mcp"),
			),
			want: "sending: Post \"http://127.0.0.1:1\": connection refused\nconfigured http://127.0.0.1:1",
		},
		{
			name: "FlattenedUserinfo",
			err:  xerrors.New(`standalone SSE request failed: Get "http://user:***@127.0.0.1:2/s/SECRETPATH/mcp?api_key=SECRETQ": EOF`),
			want: `standalone SSE request failed: Get "http://127.0.0.1:2": EOF`,
		},
		{
			name: "UppercaseScheme",
			err:  xerrors.New("dial HTTPS://mcp.example.com/SECRETPATH#frag failed"),
			want: "dial HTTPS://mcp.example.com failed",
		},
		{
			name: "SSEEndpoint",
			err:  xerrors.New(`Post "https://mcp.example.com/msg?session=SECRETQ": EOF`),
			want: `Post "https://mcp.example.com": EOF`,
		},
		{
			name: "RelativeRedirectPath",
			err: xerrors.Errorf("connect: %w", &url.Error{
				Op:  "Post",
				URL: "/s/SECRETPATH/mcp/",
				Err: xerrors.New("redirect changed method from POST to GET"),
			}),
			want: `connect: Post "<relative url>": redirect changed method from POST to GET`,
		},
		{
			name: "RelativeRedirectRoot",
			err: xerrors.Errorf("refusing redirect from http://127.0.0.1:3: %w", &url.Error{
				Op:  "Post",
				URL: "/",
				Err: xerrors.New("redirect changed method from POST to GET"),
			}),
			want: `refusing redirect from http://127.0.0.1:3: Post "<relative url>": redirect changed method from POST to GET`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := redactErrorURLs(tt.err)
			assert.Equal(t, tt.want, got)
			for _, secret := range []string{"pa55word", "SECRETPATH", "SECRETQ", "***"} {
				assert.NotContains(t, got, secret)
			}
		})
	}
}
