// Package agentacp manages workspace-local ACP harnesses and sessions.
package agentacp

import (
	"context"
	"encoding/json"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/afero"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3"
	acp "github.com/coder/acp-go-sdk"
	"github.com/coder/quartz"

	"github.com/coder/coder/v2/agent/agentexec"
	"github.com/coder/coder/v2/agent/usershell"
	"github.com/coder/coder/v2/buildinfo"
	"github.com/coder/coder/v2/codersdk/workspacesdk"
)

// Errors classify agent API failures.
var (
	ErrInvalid     = xerrors.New("invalid ACP request")
	ErrNotFound    = xerrors.New("ACP session not found")
	ErrConflict    = xerrors.New("ACP request conflict")
	ErrUnavailable = xerrors.New("ACP harness unavailable")
)

const setupTimeout = 30 * time.Second
const maxWait = 5 * time.Minute

// Options supplies the workspace execution environment and lifecycle.
type Options struct {
	Logger     slog.Logger
	Clock      quartz.Clock
	Execer     agentexec.Execer
	Filesystem afero.Fs
	EnvInfo    usershell.EnvInfoer
	UpdateEnv  func([]string) ([]string, error)
	WorkingDir func() string
}

type messageResult struct {
	text     string
	response workspacesdk.ACPMessageResponse
	err      error
}

type assistantText struct {
	seq  uint64
	text string
}

type session struct {
	interrupting uint64
	assistant    []assistantText
	opMu         sync.Mutex
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	request      workspacesdk.ACPCreateSessionRequest
	info         workspacesdk.ACPSession
	proc         *process
	harness      workspacesdk.ACPHarness
	events       []workspacesdk.ACPEvent
	lastUser     uint64
	replaying    bool
	closed       bool
	turn         uint64
	changed      chan struct{}
	ready        chan struct{}
	setupErr     error
	messages     map[uuid.UUID]messageResult
	subscribers  map[chan workspacesdk.ACPEvent]struct{}
}

// Manager owns subprocesses, catalog snapshots, and in-memory transcripts.
type Manager struct {
	configHashes  map[string][32]byte
	reloadTrigger chan struct{}
	closedDone    chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	logger        slog.Logger
	clock         quartz.Clock
	execer        agentexec.Execer
	fs            afero.Fs
	envInfo       usershell.EnvInfoer
	updateEnv     func([]string) ([]string, error)
	workingDir    func() string
	mu            sync.Mutex
	closed        bool
	requests      map[uuid.UUID]*session
	sessions      map[workspacesdk.ACPSessionID]*session
	configs       map[string]harnessConfig
	catalog       []workspacesdk.ACPHarness
	onChange      func()
	wg            sync.WaitGroup
	reloadMu      sync.Mutex
	runOnce       sync.Once
}

// NewManager creates a manager; RunDiscovery starts filesystem discovery.
// Clock, Execer, Filesystem, EnvInfo, and WorkingDir must be supplied.
// UpdateEnv is optional.
func NewManager(ctx context.Context, opts Options) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{ctx: ctx, cancel: cancel, logger: opts.Logger, clock: opts.Clock, execer: opts.Execer, fs: opts.Filesystem, envInfo: opts.EnvInfo, updateEnv: opts.UpdateEnv, workingDir: opts.WorkingDir, requests: make(map[uuid.UUID]*session), sessions: make(map[workspacesdk.ACPSessionID]*session), closedDone: make(chan struct{}), reloadTrigger: make(chan struct{}, 1), configs: make(map[string]harnessConfig), configHashes: make(map[string][32]byte)}
}

// SetOnChange installs the callback invoked after discovery changes.
func (m *Manager) SetOnChange(fn func()) { m.mu.Lock(); defer m.mu.Unlock(); m.onChange = fn }

func (m *Manager) run(fn func()) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.wg.Add(1)
	go func() { defer m.wg.Done(); fn() }()
	return true
}

func (m *Manager) timeout(ctx context.Context, duration time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	timer := m.clock.AfterFunc(duration, cancel, "acp-timeout")
	return ctx, func() { timer.Stop(); cancel() }
}

func (m *Manager) validate(req workspacesdk.ACPCreateSessionRequest) error {
	if req.HarnessSlug == "" {
		return xerrors.Errorf("%w: harness slug is required", ErrInvalid)
	}
	if req.WorkingDirectory == "" || !filepath.IsAbs(req.WorkingDirectory) {
		return xerrors.Errorf("%w: working_directory must be an explicit absolute path", ErrInvalid)
	}
	info, err := m.fs.Stat(req.WorkingDirectory)
	if err != nil || !info.IsDir() {
		return xerrors.Errorf("%w: working_directory must be an existing directory", ErrInvalid)
	}
	return nil
}

// Create creates an idle session. Request IDs deduplicate setup, not sessions.
// Setup survives caller disconnection.
func (m *Manager) Create(ctx context.Context, req workspacesdk.ACPCreateSessionRequest) (workspacesdk.ACPSession, error) {
	if req.RequestID == uuid.Nil {
		return workspacesdk.ACPSession{}, ErrInvalid
	}
	if err := m.validate(req); err != nil {
		return workspacesdk.ACPSession{}, err
	}
	req.Config = maps.Clone(req.Config)
	if len(req.Config) == 0 {
		req.Config = nil
	}
	key := req.RequestID
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return workspacesdk.ACPSession{}, ErrUnavailable
	}
	s, exists := m.requests[key]
	if exists {
		if !reflect.DeepEqual(s.request, req) {
			m.mu.Unlock()
			return workspacesdk.ACPSession{}, ErrConflict
		}
	} else {
		s = m.newSession(req, "")
		m.requests[key] = s
		m.startSetup(s, "")
	}
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return workspacesdk.ACPSession{}, ctx.Err()
	case <-s.ready:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return workspacesdk.ACPSession{}, ErrConflict
	}
	return s.infoLocked(), s.setupErr
}

// newSession and startSetup are called with m.mu held.
func (m *Manager) newSession(req workspacesdk.ACPCreateSessionRequest, nativeID string) *session {
	sctx, cancel := context.WithCancel(m.ctx)
	return &session{
		ctx: sctx, cancel: cancel, request: req,
		info: workspacesdk.ACPSession{
			ID:     workspacesdk.ACPSessionID{HarnessSlug: req.HarnessSlug, WorkingDirectory: req.WorkingDirectory, SessionID: nativeID},
			Config: maps.Clone(req.Config), Status: "starting",
			Cursor: workspacesdk.ACPCursor{Epoch: uuid.New()}, HistoryComplete: nativeID == "",
		},
		changed: make(chan struct{}), ready: make(chan struct{}),
		messages: make(map[uuid.UUID]messageResult), subscribers: make(map[chan workspacesdk.ACPEvent]struct{}),
	}
}

func (m *Manager) startSetup(s *session, nativeID string) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		s.opMu.Lock()
		defer s.opMu.Unlock()
		err := m.setup(s, nativeID)
		if err == nil && nativeID == "" {
			m.mu.Lock()
			s.mu.Lock()
			key := s.info.ID
			switch {
			case m.closed || s.closed:
				err = ErrUnavailable
			case m.sessions[key] != nil:
				err = ErrConflict
			default:
				m.sessions[key] = s
			}
			s.mu.Unlock()
			m.mu.Unlock()
		}
		if err != nil {
			s.mu.Lock()
			p := s.proc
			s.mu.Unlock()
			if p != nil {
				p.close()
			}
		}
		s.mu.Lock()
		s.setupErr = err
		if err != nil {
			s.failLocked(err)
		}
		close(s.ready)
		s.mu.Unlock()
	}()
}

func (m *Manager) setup(s *session, nativeID string) error {
	if err := m.validate(s.request); err != nil {
		return err
	}
	m.mu.Lock()
	cfg, ok := m.configs[s.request.HarnessSlug]
	m.mu.Unlock()
	if !ok {
		return xerrors.Errorf("%w: harness is missing or disabled", ErrUnavailable)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrUnavailable
	}
	retained := s.info.HistoryComplete || s.lastUser != 0
	old := s.proc
	s.proc = nil
	s.info.ID.SessionID = nativeID
	s.mu.Unlock()
	if old != nil {
		old.close()
	}
	p, err := m.launch(s, cfg)
	if err != nil {
		return xerrors.Errorf("%w: %v", ErrUnavailable, err)
	}
	ok = false
	defer func() {
		if !ok {
			p.close()
		}
	}()
	ctx, cancel := m.timeout(s.ctx, setupTimeout)
	defer cancel()
	init, err := p.conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, ClientInfo: &acp.Implementation{Name: "coder-agent", Version: buildinfo.Version()}})
	if err != nil {
		return xerrors.Errorf("%w: initialize harness: %v", ErrUnavailable, p.failure(err, cfg))
	}
	if init.ProtocolVersion != acp.ProtocolVersionNumber {
		return xerrors.Errorf("%w: unsupported ACP protocol version", ErrUnavailable)
	}
	harness := harnessInfo(cfg, init)
	var options []acp.SessionConfigOption
	switch {
	case nativeID == "":
		created, err := p.conn.NewSession(ctx, acp.NewSessionRequest{Cwd: s.request.WorkingDirectory, McpServers: []acp.McpServer{}})
		if err != nil {
			return xerrors.Errorf("%w: create ACP session: %v", ErrUnavailable, err)
		}
		if created.SessionId == "" {
			return xerrors.Errorf("%w: harness returned an empty session id", ErrUnavailable)
		}
		nativeID, options = string(created.SessionId), created.ConfigOptions
	case harness.LoadSession && (!retained || !harness.ResumeSession):
		s.mu.Lock()
		s.replaying = true
		s.events = nil
		s.assistant = nil
		s.lastUser = 0
		s.info.Cursor = workspacesdk.ACPCursor{Epoch: uuid.New()}
		s.appendLocked("reset", "", nil)
		s.mu.Unlock()
		loaded, err := p.conn.LoadSession(ctx, acp.LoadSessionRequest{SessionId: acp.SessionId(nativeID), Cwd: s.request.WorkingDirectory, McpServers: []acp.McpServer{}})
		s.mu.Lock()
		s.replaying = false
		s.info.HistoryComplete = err == nil
		s.mu.Unlock()
		if err != nil {
			return xerrors.Errorf("%w: load ACP session: %v", ErrUnavailable, err)
		}
		options = loaded.ConfigOptions
	case harness.ResumeSession:
		resumed, err := p.conn.ResumeSession(ctx, acp.ResumeSessionRequest{SessionId: acp.SessionId(nativeID), Cwd: s.request.WorkingDirectory, McpServers: []acp.McpServer{}})
		if err != nil {
			return xerrors.Errorf("%w: resume ACP session: %v", ErrUnavailable, err)
		}
		options = resumed.ConfigOptions
	default:
		return xerrors.Errorf("%w: harness does not support restoration", ErrUnavailable)
	}
	for _, id := range slices.Sorted(maps.Keys(s.request.Config)) {
		value := s.request.Config[id]
		if !validConfig(options, id, value) {
			return xerrors.Errorf("%w: unsupported configuration %q=%q", ErrInvalid, id, value)
		}
		updated, err := p.conn.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{SessionId: acp.SessionId(nativeID), ConfigId: acp.SessionConfigId(id), Value: acp.SessionConfigValueId(value)}})
		if err != nil {
			return xerrors.Errorf("%w: configure ACP session: %v", ErrInvalid, err)
		}
		options = updated.ConfigOptions
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrUnavailable
	}
	s.proc, s.harness = p, harness
	s.info.ID.SessionID = nativeID
	s.info.HarnessDisplayName = cfg.displayName
	s.info.Harness = cloneHarness(harness)
	s.info.Harness.ConfigOptions = configOptions(options)
	s.info.Status, s.info.Error, s.info.StopReason = "idle", "", ""
	s.appendLocked("status", "", nil)
	s.mu.Unlock()
	ok = m.run(func() {
		select {
		case <-p.done:
		case <-p.conn.Done():
		case <-s.ctx.Done():
		}
		p.close()
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.closed && s.proc == p {
			err := p.err
			if err == nil {
				err = xerrors.New("harness disconnected")
			}
			s.failLocked(p.failure(err, cfg))
		}
	})
	if !ok {
		return ErrUnavailable
	}
	return nil
}

func validConfig(options []acp.SessionConfigOption, id, value string) bool {
	for _, opt := range configOptions(options) {
		if opt.ID == id {
			for _, v := range opt.Values {
				if v.ID == value {
					return true
				}
			}
		}
	}
	return false
}

// get loads absent sessions using their native identity. Existing failed
// runtimes remain observable so waits can report failed work.
func (m *Manager) get(ctx context.Context, id workspacesdk.ACPSessionID) (*session, error) {
	if id.SessionID == "" || id.HarnessSlug == "" || !filepath.IsAbs(id.WorkingDirectory) {
		return nil, ErrInvalid
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrUnavailable
	}
	s, exists := m.sessions[id]
	if !exists {
		req := workspacesdk.ACPCreateSessionRequest{HarnessSlug: id.HarnessSlug, WorkingDirectory: id.WorkingDirectory}
		s = m.newSession(req, id.SessionID)
		m.sessions[id] = s
		m.startSetup(s, id.SessionID)
	}
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ready:
	}
	s.mu.Lock()
	setupErr, closed := s.setupErr, s.closed
	s.mu.Unlock()
	if closed {
		return nil, ErrNotFound
	}
	if !exists && setupErr != nil {
		return nil, setupErr
	}
	return s, setupErr
}

// recover loads missing sessions and recovers failed runtimes for new work or
// transcript access. It never replays previously admitted messages.
func (m *Manager) recover(ctx context.Context, id workspacesdk.ACPSessionID) (*session, error) {
	s, err := m.get(ctx, id)
	if err != nil && s == nil {
		return nil, err
	}
	s.mu.Lock()
	failed := s.info.Status == "error"
	s.mu.Unlock()
	if !failed {
		return s, nil
	}
	done := make(chan error, 1)
	if !m.run(func() {
		s.opMu.Lock()
		defer s.opMu.Unlock()
		s.mu.Lock()
		failed, closed := s.info.Status == "error", s.closed
		s.mu.Unlock()
		if closed {
			done <- ErrNotFound
			return
		}
		if !failed {
			done <- nil
			return
		}
		err := m.setup(s, id.SessionID)
		s.mu.Lock()
		s.setupErr = err
		if err != nil {
			s.failLocked(err)
		}
		s.mu.Unlock()
		done <- err
	}) {
		return nil, ErrUnavailable
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-done:
		return s, err
	}
}

// Message submits work or steers an active turn, deduplicating message IDs.
func (m *Manager) Message(ctx context.Context, id workspacesdk.ACPSessionID, req workspacesdk.ACPMessageRequest) (workspacesdk.ACPMessageResponse, error) {
	if req.ID == uuid.Nil || strings.TrimSpace(req.Text) == "" {
		return workspacesdk.ACPMessageResponse{}, ErrInvalid
	}
	s, err := m.recover(ctx, id)
	if err != nil {
		return workspacesdk.ACPMessageResponse{}, err
	}
	select {
	case <-s.ready:
	case <-ctx.Done():
		return workspacesdk.ACPMessageResponse{}, ctx.Err()
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if result, ok := s.messages[req.ID]; ok {
		s.mu.Unlock()
		if result.text != req.Text {
			return workspacesdk.ACPMessageResponse{}, ErrConflict
		}
		return result.response, result.err
	}
	if s.closed || s.proc == nil || s.info.Status == "error" {
		s.mu.Unlock()
		return workspacesdk.ACPMessageResponse{}, ErrUnavailable
	}
	busy := s.info.Status == "running"
	if busy && !s.harness.Steering {
		s.mu.Unlock()
		return workspacesdk.ACPMessageResponse{}, xerrors.Errorf("%w: harness does not support steering", ErrConflict)
	}
	p := s.proc
	s.appendLocked("user_message", req.Text, nil, req.ID)
	submittedSeq := s.info.Cursor.Seq
	if !busy {
		s.lastUser = submittedSeq
		s.info.Status, s.info.Error, s.info.StopReason = "running", "", ""
		s.appendLocked("status", "", nil)
		s.turn++
	}
	turn, nativeID := s.turn, s.info.ID.SessionID
	s.mu.Unlock()
	response := workspacesdk.ACPMessageResponse{Outcome: "prompt"}
	if busy {
		steerCtx, cancel := m.timeout(s.ctx, setupTimeout)
		defer cancel()
		var raw json.RawMessage
		raw, err = p.conn.CallExtension(steerCtx, "_session/steering", map[string]any{"sessionId": nativeID, "prompt": []acp.ContentBlock{acp.TextBlock(req.Text)}})
		if err == nil {
			err = json.Unmarshal(raw, &response)
		}
		if err == nil && response.Outcome != "injected" && response.Outcome != "startedNewTurn" {
			err = xerrors.Errorf("%w: steering was not accepted", ErrConflict)
		}
	} else {
		written := p.expectPrompt()
		completed := make(chan struct{})
		if !m.run(func() {
			defer close(completed)
			result, err := p.conn.Prompt(s.ctx, acp.PromptRequest{SessionId: acp.SessionId(nativeID), Prompt: []acp.ContentBlock{acp.TextBlock(req.Text)}})
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.closed || s.proc != p || s.turn != turn {
				return
			}
			if err != nil {
				s.failLocked(err)
				return
			}
			s.info.Status, s.info.StopReason = "idle", string(result.StopReason)
			s.appendLocked("status", "", nil)
		}) {
			err = ErrUnavailable
		} else {
			select {
			case <-written:
			case <-completed:
			case <-s.ctx.Done():
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if busy && err == nil {
		s.lastUser = submittedSeq
	}
	s.messages[req.ID] = messageResult{text: req.Text, response: response, err: err}
	if err != nil {
		s.appendLocked("error", err.Error(), nil)
	}
	return response, err
}

// Interrupt cancels the current prompt without terminating the subprocess.
func (m *Manager) Interrupt(ctx context.Context, id workspacesdk.ACPSessionID) (workspacesdk.ACPSession, error) {
	s, err := m.recover(ctx, id)
	if err != nil {
		return workspacesdk.ACPSession{}, err
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	info, p, turn, changed := s.infoLocked(), s.proc, s.turn, s.changed
	start := info.Status == "running" && p != nil && s.interrupting != turn
	if start {
		s.interrupting = turn
	}
	s.mu.Unlock()
	if !start {
		return info, nil
	}
	err = p.conn.Cancel(ctx, acp.CancelNotification{SessionId: acp.SessionId(info.ID.SessionID)})
	if err != nil {
		s.mu.Lock()
		s.interrupting = 0
		s.mu.Unlock()
		return info, err
	}
	// A prompt request is handled asynchronously by the peer. Repeat cancellation
	// on updates or a bounded timer until the admitted turn has actually stopped.
	if !m.run(func() {
		defer func() {
			s.mu.Lock()
			if s.interrupting == turn {
				s.interrupting = 0
			}
			s.mu.Unlock()
		}()
		for {
			s.mu.Lock()
			if s.closed || s.proc != p || s.turn != turn || s.info.Status != "running" {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			timer := m.clock.NewTimer(100*time.Millisecond, "acp-interrupt")
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-changed:
			case <-timer.C:
			}
			timer.Stop()
			s.mu.Lock()
			active := !s.closed && s.proc == p && s.turn == turn && s.info.Status == "running"
			changed = s.changed
			s.mu.Unlock()
			if !active {
				return
			}
			if p.conn.Cancel(s.ctx, acp.CancelNotification{SessionId: acp.SessionId(info.ID.SessionID)}) != nil {
				return
			}
		}
	}) {
		return info, ErrUnavailable
	}
	return info, nil
}

func (s *session) infoLocked() workspacesdk.ACPSession {
	info := s.info
	info.Config = maps.Clone(info.Config)
	info.Harness = cloneHarness(info.Harness)
	return info
}

func (s *session) appendLocked(kind, text string, update json.RawMessage, messageIDs ...uuid.UUID) {
	s.info.Cursor.Seq++
	e := workspacesdk.ACPEvent{Cursor: s.info.Cursor, Kind: kind, Text: text, Update: update}
	if len(messageIDs) > 0 {
		id := messageIDs[0]
		e.MessageID = &id
	}
	if kind == "status" || kind == "reset" {
		info := s.infoLocked()
		e.Session = &info
	}
	s.events = append(s.events, e)
	close(s.changed)
	s.changed = make(chan struct{})
	for ch := range s.subscribers {
		select {
		case ch <- cloneEvent(e):
		default:
			close(ch)
			delete(s.subscribers, ch)
		}
	}
}

func (s *session) failLocked(err error) {
	s.info.Status, s.info.Error = "error", err.Error()
	s.appendLocked("error", err.Error(), nil)
	s.appendLocked("status", "", nil)
}

func (s *session) readLocked(after *workspacesdk.ACPCursor, start uint64) (workspacesdk.ACPSessionResponse, error) {
	if after != nil {
		if after.Epoch != s.info.Cursor.Epoch || after.Seq > s.info.Cursor.Seq {
			return workspacesdk.ACPSessionResponse{}, xerrors.Errorf("%w: transcript cursor requires reset", ErrConflict)
		}
		start = after.Seq
	}
	r := workspacesdk.ACPSessionResponse{Session: s.infoLocked(), Events: make([]workspacesdk.ACPEvent, 0, len(s.events[start:]))}
	for _, event := range s.events[start:] {
		r.Events = append(r.Events, cloneEvent(event))
	}
	var text strings.Builder
	for _, part := range s.assistant {
		if part.seq > start {
			_, _ = text.WriteString(part.text)
		}
	}
	r.AssistantResponse = text.String()
	return r, nil
}

// Read returns a consistent copy of the available transcript.
func (m *Manager) Read(ctx context.Context, id workspacesdk.ACPSessionID, after *workspacesdk.ACPCursor) (workspacesdk.ACPSessionResponse, error) {
	s, err := m.recover(ctx, id)
	if err != nil {
		return workspacesdk.ACPSessionResponse{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return workspacesdk.ACPSessionResponse{}, ErrNotFound
	}
	return s.readLocked(after, 0)
}

// List returns all registered runtime sessions.
func (m *Manager) List() []workspacesdk.ACPSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []workspacesdk.ACPSession{}
	for _, s := range m.sessions {
		s.mu.Lock()
		out = append(out, s.infoLocked())
		s.mu.Unlock()
	}
	slices.SortFunc(out, func(a, b workspacesdk.ACPSession) int {
		if a.ID.HarnessSlug != b.ID.HarnessSlug {
			return strings.Compare(a.ID.HarnessSlug, b.ID.HarnessSlug)
		}
		if a.ID.WorkingDirectory != b.ID.WorkingDirectory {
			return strings.Compare(a.ID.WorkingDirectory, b.ID.WorkingDirectory)
		}
		return strings.Compare(a.ID.SessionID, b.ID.SessionID)
	})
	return out
}

// Wait bounds only observation, never the prompt's lifetime.
func (m *Manager) Wait(ctx context.Context, id workspacesdk.ACPSessionID, opts workspacesdk.ACPWaitOptions) (workspacesdk.ACPSessionResponse, error) {
	if opts.Timeout < 0 || opts.Timeout > maxWait {
		return workspacesdk.ACPSessionResponse{}, ErrInvalid
	}
	if opts.Timeout == 0 {
		opts.Timeout = maxWait
	}
	s, err := m.get(ctx, id)
	if err != nil {
		return workspacesdk.ACPSessionResponse{}, err
	}
	timer := m.clock.NewTimer(opts.Timeout, "acp-wait")
	defer timer.Stop()
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return workspacesdk.ACPSessionResponse{}, ErrUnavailable
		}
		if s.info.Status != "running" && s.info.Status != "starting" {
			r, err := s.readLocked(opts.After, s.latestUserStart())
			s.mu.Unlock()
			return r, err
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return workspacesdk.ACPSessionResponse{}, ctx.Err()
		case <-timer.C:
			s.mu.Lock()
			r, err := s.readLocked(opts.After, s.latestUserStart())
			r.TimedOut = s.info.Status == "running" || s.info.Status == "starting"
			s.mu.Unlock()
			return r, err
		case <-ch:
		}
	}
}

// Subscribe atomically returns history and a subscription to later events.
func (m *Manager) Subscribe(ctx context.Context, id workspacesdk.ACPSessionID, after *workspacesdk.ACPCursor) ([]workspacesdk.ACPEvent, <-chan workspacesdk.ACPEvent, func(), error) {
	s, err := m.recover(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, nil, ErrUnavailable
	}
	r, err := s.readLocked(after, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	ch := make(chan workspacesdk.ACPEvent, 64)
	s.subscribers[ch] = struct{}{}
	info := s.infoLocked()
	r.Events = append(r.Events, workspacesdk.ACPEvent{Kind: "snapshot", Cursor: info.Cursor, Session: &info})
	return r.Events, ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.subscribers[ch]; ok {
			delete(s.subscribers, ch)
			close(ch)
		}
	}, nil
}

func (s *session) shutdown() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	p := s.proc
	close(s.changed)
	s.changed = make(chan struct{})
	for ch := range s.subscribers {
		close(ch)
		delete(s.subscribers, ch)
	}
	s.mu.Unlock()
	if p != nil {
		p.close()
	}
}

// Close stops subprocesses, discovery, and all manager-owned goroutines.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		<-m.closedDone
		return nil
	}
	m.closed = true
	m.cancel()
	sessions := slices.Collect(maps.Values(m.sessions))
	// Pending creations have not received a native session ID yet.
	for _, s := range m.requests {
		if !slices.Contains(sessions, s) {
			sessions = append(sessions, s)
		}
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.shutdown()
	}
	m.wg.Wait()
	close(m.closedDone)
	return nil
}

func harnessInfo(cfg harnessConfig, init acp.InitializeResponse) workspacesdk.ACPHarness {
	steering, _ := init.Meta["steering"].(map[string]any)
	supported, _ := steering["supported"].(bool)
	return workspacesdk.ACPHarness{Slug: cfg.slug, DisplayName: cfg.displayName, LoadSession: init.AgentCapabilities.LoadSession, ResumeSession: init.AgentCapabilities.SessionCapabilities.Resume != nil, Steering: supported, ConfigOptions: []workspacesdk.ACPConfigOption{}}
}

func configOptions(options []acp.SessionConfigOption) []workspacesdk.ACPConfigOption {
	out := []workspacesdk.ACPConfigOption{}
	for _, option := range options {
		s := option.Select
		if s == nil {
			continue
		}
		opt := workspacesdk.ACPConfigOption{ID: string(s.Id), Name: s.Name, CurrentValue: string(s.CurrentValue), Values: []workspacesdk.ACPConfigValue{}}
		if s.Category != nil {
			opt.Category = string(*s.Category)
		}
		if s.Description != nil {
			opt.Description = *s.Description
		}
		add := func(v acp.SessionConfigSelectOption, group, groupName string) {
			value := workspacesdk.ACPConfigValue{ID: string(v.Value), Name: v.Name, Group: group, GroupName: groupName}
			if v.Description != nil {
				value.Description = *v.Description
			}
			opt.Values = append(opt.Values, value)
		}
		if s.Options.Ungrouped != nil {
			for _, v := range *s.Options.Ungrouped {
				add(v, "", "")
			}
		}
		if s.Options.Grouped != nil {
			for _, group := range *s.Options.Grouped {
				for _, v := range group.Options {
					add(v, string(group.Group), group.Name)
				}
			}
		}
		out = append(out, opt)
	}
	return out
}

func cloneEvent(e workspacesdk.ACPEvent) workspacesdk.ACPEvent {
	e.Update = slices.Clone(e.Update)
	if e.MessageID != nil {
		id := *e.MessageID
		e.MessageID = &id
	}
	if e.Session != nil {
		info := *e.Session
		info.Config = maps.Clone(info.Config)
		info.Harness = cloneHarness(info.Harness)
		e.Session = &info
	}
	return e
}

func cloneHarness(h workspacesdk.ACPHarness) workspacesdk.ACPHarness {
	h.ConfigOptions = slices.Clone(h.ConfigOptions)
	for i := range h.ConfigOptions {
		h.ConfigOptions[i].Values = slices.Clone(h.ConfigOptions[i].Values)
	}
	return h
}

func (s *session) latestUserStart() uint64 {
	if s.lastUser > 0 {
		return s.lastUser - 1
	}
	return 0
}
