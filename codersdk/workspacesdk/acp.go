package workspacesdk

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3"

	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/coder/v2/codersdk/wsjson"
	"github.com/coder/websocket"
)

// ACPConfigValue is a selectable value advertised by a harness.
type ACPConfigValue struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Group       string `json:"group,omitempty"`
	GroupName   string `json:"group_name,omitempty"`
}

// ACPConfigOption describes an ACP session select option.
type ACPConfigOption struct {
	ID           string           `json:"id"`
	Name         string           `json:"name"`
	Description  string           `json:"description,omitempty"`
	CurrentValue string           `json:"current_value"`
	Category     string           `json:"category,omitempty"`
	Values       []ACPConfigValue `json:"values"`
}

// ACPHarness is the public discovery result. Commands stay on the agent.
type ACPHarness struct {
	Slug          string            `json:"slug"`
	DisplayName   string            `json:"display_name"`
	LoadSession   bool              `json:"load_session"`
	ResumeSession bool              `json:"resume_session"`
	Steering      bool              `json:"steering"`
	ConfigOptions []ACPConfigOption `json:"config_options"`
	Error         string            `json:"error,omitempty"`
}

// ACPCursor identifies a position in an in-memory transcript.
type ACPCursor struct {
	Epoch uuid.UUID `json:"epoch"`
	Seq   uint64    `json:"seq"`
}

// ACPSessionID identifies a native session and supplies its recovery context.
// The agent uses it to load or resume sessions without local metadata storage.
type ACPSessionID struct {
	HarnessSlug      string `json:"harness_slug"`
	WorkingDirectory string `json:"working_directory"`
	SessionID        string `json:"session_id"`
}

// ACPCreateSessionRequest creates an idle session in an explicit directory.
type ACPCreateSessionRequest struct {
	// RequestID deduplicates creation within an agent lifetime.
	RequestID        uuid.UUID         `json:"request_id"`
	HarnessSlug      string            `json:"harness_slug"`
	WorkingDirectory string            `json:"working_directory"`
	Config           map[string]string `json:"config,omitempty"`
}

// ACPMessageRequest identifies a message so retries do not repeat its work.
type ACPMessageRequest struct {
	ID   uuid.UUID `json:"id"`
	Text string    `json:"text"`
}

// ACPMessageResponse reports message admission, not turn completion.
type ACPMessageResponse struct {
	Outcome string `json:"outcome"`
}

// ACPSession describes a runtime session without its transcript.
type ACPSession struct {
	Harness            ACPHarness        `json:"harness"`
	ID                 ACPSessionID      `json:"id"`
	HarnessDisplayName string            `json:"harness_display_name"`
	Config             map[string]string `json:"config,omitempty"`
	Status             string            `json:"status"`
	StopReason         string            `json:"stop_reason,omitempty"`
	Error              string            `json:"error,omitempty"`
	Cursor             ACPCursor         `json:"cursor"`
	HistoryComplete    bool              `json:"history_complete"`
}

// ACPEvent preserves ACP updates separately from locally submitted messages.
type ACPEvent struct {
	MessageID *uuid.UUID      `json:"message_id,omitempty"`
	Cursor    ACPCursor       `json:"cursor"`
	Kind      string          `json:"kind"`
	Text      string          `json:"text,omitempty"`
	Update    json.RawMessage `json:"update,omitempty"`
	Session   *ACPSession     `json:"session,omitempty"`
}

// ACPSessionResponse carries rich history and its assistant-text projection.
type ACPSessionResponse struct {
	Session           ACPSession `json:"session"`
	Events            []ACPEvent `json:"events"`
	AssistantResponse string     `json:"assistant_response"`
	TimedOut          bool       `json:"timed_out"`
}

// ACPReadOptions optionally selects the portion after a transcript cursor.
type ACPReadOptions struct {
	After *ACPCursor
}

// ACPWaitOptions bounds a wait independently of the session's lifetime.
type ACPWaitOptions struct {
	After   *ACPCursor
	Timeout time.Duration
}

func acpQuery(id ACPSessionID, after *ACPCursor) url.Values {
	v := url.Values{
		"harness_slug":      {id.HarnessSlug},
		"working_directory": {id.WorkingDirectory},
		"session_id":        {id.SessionID},
	}
	if after != nil {
		v.Set("epoch", after.Epoch.String())
		v.Set("after", strconv.FormatUint(after.Seq, 10))
	}
	return v
}

func acpRequest[T any](ctx context.Context, c *agentConn, method, path string, body any) (T, error) {
	var result T
	res, err := c.apiRequest(ctx, method, "/api/v0/acp"+path, body)
	if err != nil {
		return result, xerrors.Errorf("request ACP: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusAccepted {
		return result, codersdk.ReadBodyAsError(res)
	}
	return result, decodeAgentJSON(res, &result)
}

// ListACPHarnesses returns the cached agent-local harness catalog.
func (c *agentConn) ListACPHarnesses(ctx context.Context) ([]ACPHarness, error) {
	return acpRequest[[]ACPHarness](ctx, c, http.MethodGet, "/harnesses", nil)
}

// CreateACPSession creates an idle ACP session before its first prompt.
func (c *agentConn) CreateACPSession(ctx context.Context, req ACPCreateSessionRequest) (ACPSession, error) {
	return acpRequest[ACPSession](ctx, c, http.MethodPost, "/sessions", req)
}

// ListACPSessions lists all registered agent-local runtime sessions.
func (c *agentConn) ListACPSessions(ctx context.Context) ([]ACPSession, error) {
	return acpRequest[[]ACPSession](ctx, c, http.MethodGet, "/sessions", nil)
}

// ReadACPSession returns the available transcript and runtime status.
func (c *agentConn) ReadACPSession(ctx context.Context, id ACPSessionID, opts ACPReadOptions) (ACPSessionResponse, error) {
	return acpRequest[ACPSessionResponse](ctx, c, http.MethodGet, "/sessions/session?"+acpQuery(id, opts.After).Encode(), nil)
}

// SendACPMessage admits a prompt or steering message using its deduplication ID.
func (c *agentConn) SendACPMessage(ctx context.Context, id ACPSessionID, req ACPMessageRequest) (ACPMessageResponse, error) {
	return acpRequest[ACPMessageResponse](ctx, c, http.MethodPost, "/sessions/session/messages?"+acpQuery(id, nil).Encode(), req)
}

// InterruptACPSession requests cancellation while retaining the subprocess.
func (c *agentConn) InterruptACPSession(ctx context.Context, id ACPSessionID) (ACPSession, error) {
	return acpRequest[ACPSession](ctx, c, http.MethodPost, "/sessions/session/interrupt?"+acpQuery(id, nil).Encode(), nil)
}

// WaitACPSession waits for completion independently of the prompt's lifetime.
func (c *agentConn) WaitACPSession(ctx context.Context, id ACPSessionID, opts ACPWaitOptions) (ACPSessionResponse, error) {
	q := acpQuery(id, opts.After)
	if opts.Timeout != 0 {
		q.Set("timeout_ms", strconv.FormatInt(opts.Timeout.Milliseconds(), 10))
	}
	return acpRequest[ACPSessionResponse](ctx, c, http.MethodGet, "/sessions/session/wait?"+q.Encode(), nil)
}

// WatchACPSession replays history and streams subsequent ordered events.
func (c *agentConn) WatchACPSession(ctx context.Context, logger slog.Logger, id ACPSessionID, opts ACPReadOptions) (<-chan ACPEvent, io.Closer, error) {
	host := net.JoinHostPort(c.agentAddress().String(), strconv.Itoa(AgentHTTPAPIServerPort))
	c.headersMu.RLock()
	headers := c.extraHeaders.Clone()
	c.headersMu.RUnlock()
	u := "http://" + host + "/api/v0/acp/sessions/session/stream?" + acpQuery(id, opts.After).Encode()
	conn, res, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: c.apiClient(ctx), HTTPHeader: headers})
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	if err != nil {
		if res != nil {
			return nil, nil, codersdk.ReadBodyAsError(res)
		}
		return nil, nil, err
	}
	conn.SetReadLimit(16 << 20)
	d := wsjson.NewDecoder[ACPEvent](conn, websocket.MessageText, logger)
	return d.Chan(), d, nil
}
