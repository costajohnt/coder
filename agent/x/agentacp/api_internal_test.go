package agentacp

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/coder/coder/v2/codersdk/workspacesdk"
	"github.com/coder/coder/v2/testutil"
)

func TestAPIAndStreamReplay(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, "both")
	handler := http.StripPrefix("/api/v0/acp", NewAPI(m).Routes())
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx := testutil.Context(t, testutil.WaitLong)
	type testResponse struct {
		StatusCode int
		Body       io.Reader
	}
	call := func(method, path string, body any, header string) testResponse {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(ctx, method, server.URL+"/api/v0/acp"+path, bytes.NewReader(raw))
		require.NoError(t, err)
		req.Header.Set(workspacesdk.CoderChatIDHeader, header)
		res, err := server.Client().Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		bodyBytes, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return testResponse{StatusCode: res.StatusCode, Body: bytes.NewReader(bodyBytes)}
	}
	for _, header := range []string{"", "invalid", uuid.Nil.String()} {
		require.Equal(t, http.StatusOK, call("GET", "/harnesses", nil, header).StatusCode)
	}
	req := workspacesdk.ACPCreateSessionRequest{RequestID: uuid.New(), HarnessSlug: "fake", WorkingDirectory: dir}
	res := call("POST", "/sessions", req, "")
	require.Equal(t, http.StatusOK, res.StatusCode)
	var info workspacesdk.ACPSession
	require.NoError(t, json.NewDecoder(res.Body).Decode(&info))
	res = call("POST", "/sessions", req, uuid.NewString())
	require.Equal(t, http.StatusOK, res.StatusCode)
	var duplicate workspacesdk.ACPSession
	require.NoError(t, json.NewDecoder(res.Body).Decode(&duplicate))
	require.Equal(t, info.ID, duplicate.ID)
	q := url.Values{"harness_slug": {info.ID.HarnessSlug}, "working_directory": {info.ID.WorkingDirectory}, "session_id": {info.ID.SessionID}}
	path := func(suffix string) string { return "/sessions/session" + suffix + "?" + q.Encode() }
	for _, header := range []string{"", "invalid", uuid.Nil.String(), uuid.NewString()} {
		res := call("GET", path(""), nil, header)
		require.Equal(t, http.StatusOK, res.StatusCode)
		var read workspacesdk.ACPSessionResponse
		require.NoError(t, json.NewDecoder(res.Body).Decode(&read))
		require.Equal(t, info.ID, read.Session.ID)
		require.Equal(t, info.Cursor, read.Session.Cursor)
	}
	require.Equal(t, http.StatusOK, call("GET", path(""), nil, "").StatusCode)
	require.Equal(t, http.StatusBadRequest, call("GET", path("/wait")+"&timeout_ms=-1", nil, "").StatusCode)
	message := workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hello"}
	require.Equal(t, http.StatusOK, call("POST", path("/messages"), message, "").StatusCode)
	require.Equal(t, http.StatusOK, call("POST", path("/messages"), message, uuid.NewString()).StatusCode)
	res = call("GET", path("/wait"), nil, "")
	require.Equal(t, http.StatusOK, res.StatusCode)
	var result workspacesdk.ACPSessionResponse
	require.NoError(t, json.NewDecoder(res.Body).Decode(&result))
	require.Equal(t, "answer:hello:default", result.AssistantResponse)
	userMessages := 0
	for _, event := range result.Events {
		if event.Kind == "user_message" {
			userMessages++
		}
	}
	require.Equal(t, 1, userMessages)
	res = call("GET", "/sessions", nil, "")
	require.Equal(t, http.StatusOK, res.StatusCode)
	var listed []workspacesdk.ACPSession
	require.NoError(t, json.NewDecoder(res.Body).Decode(&listed))
	require.Len(t, listed, 1)
	require.Equal(t, info.ID, listed[0].ID)
	streamURL := strings.Replace(server.URL, "http://", "ws://", 1) + "/api/v0/acp" + path("/stream")
	conn, wsResponse, err := websocket.Dial(ctx, streamURL, nil)
	require.NoError(t, err)
	if wsResponse != nil && wsResponse.Body != nil {
		_ = wsResponse.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()
	var last uint64
	for {
		var event workspacesdk.ACPEvent
		require.NoError(t, wsjson.Read(ctx, conn, &event))
		require.GreaterOrEqual(t, event.Cursor.Seq, last)
		last = event.Cursor.Seq
		if event.Kind == "snapshot" {
			break
		}
	}
	require.Equal(t, result.Session.Cursor.Seq, last)
	_ = conn.CloseNow()
	replayURL := streamURL + "&epoch=" + result.Session.Cursor.Epoch.String() + "&after=0"
	conn, wsResponse, err = websocket.Dial(ctx, replayURL, nil)
	require.NoError(t, err)
	if wsResponse != nil && wsResponse.Body != nil {
		_ = wsResponse.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()
	var event workspacesdk.ACPEvent
	require.NoError(t, wsjson.Read(ctx, conn, &event))
	require.Equal(t, uint64(1), event.Cursor.Seq)
	require.Equal(t, http.StatusMethodNotAllowed, call("DELETE", path(""), nil, "").StatusCode)
	require.Equal(t, http.StatusOK, call("GET", path(""), nil, "").StatusCode)
}
