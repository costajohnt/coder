package chatstate_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"cdr.dev/slog/v3/sloggers/slogtest"
	"github.com/coder/coder/v2/coderd/database"
	"github.com/coder/coder/v2/coderd/database/dbauthz"
	"github.com/coder/coder/v2/coderd/rbac"
	"github.com/coder/coder/v2/coderd/x/chatd/chatstate"
	"github.com/coder/coder/v2/testutil"
)

// roundTripCounts records the chat reads issued inside a transaction so
// tests can assert what runs while the transition lock is held.
type roundTripCounts struct {
	mu             sync.Mutex
	lock           int
	chatWrites     int
	getChatByID    int
	countQueued    int
	heartbeatStale int
}

func (c *roundTripCounts) inc(field *int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	*field++
}

// countingStore counts chat reads only on the transactional handle
// handed to InTx callbacks, so setup reads on the root store do not
// pollute the numbers.
type countingStore struct {
	database.Store
	counts *roundTripCounts
	inTx   bool
}

func (s *countingStore) InTx(fn func(database.Store) error, opts *database.TxOptions) error {
	return s.Store.InTx(func(tx database.Store) error {
		return fn(&countingStore{Store: tx, counts: s.counts, inTx: true})
	}, opts)
}

func (s *countingStore) LockChatForTransition(ctx context.Context, id uuid.UUID) (database.Chat, error) {
	if s.inTx {
		s.counts.inc(&s.counts.lock)
	}
	return s.Store.LockChatForTransition(ctx, id)
}

func (s *countingStore) UpdateChatExecutionState(ctx context.Context, arg database.UpdateChatExecutionStateParams) (database.UpdateChatExecutionStateRow, error) {
	if s.inTx {
		s.counts.inc(&s.counts.chatWrites)
	}
	return s.Store.UpdateChatExecutionState(ctx, arg)
}

func (s *countingStore) BumpChatSnapshotVersion(ctx context.Context, arg database.BumpChatSnapshotVersionParams) (database.BumpChatSnapshotVersionRow, error) {
	if s.inTx {
		s.counts.inc(&s.counts.chatWrites)
	}
	return s.Store.BumpChatSnapshotVersion(ctx, arg)
}

func (s *countingStore) IncrementChatGenerationAttempt(ctx context.Context, arg database.IncrementChatGenerationAttemptParams) (database.IncrementChatGenerationAttemptRow, error) {
	if s.inTx {
		s.counts.inc(&s.counts.chatWrites)
	}
	return s.Store.IncrementChatGenerationAttempt(ctx, arg)
}

func (s *countingStore) UpdateChatRetryState(ctx context.Context, arg database.UpdateChatRetryStateParams) (database.UpdateChatRetryStateRow, error) {
	if s.inTx {
		s.counts.inc(&s.counts.chatWrites)
	}
	return s.Store.UpdateChatRetryState(ctx, arg)
}

func (s *countingStore) GetChatByID(ctx context.Context, id uuid.UUID) (database.Chat, error) {
	if s.inTx {
		s.counts.inc(&s.counts.getChatByID)
	}
	return s.Store.GetChatByID(ctx, id)
}

func (s *countingStore) CountChatQueuedMessages(ctx context.Context, id uuid.UUID) (int64, error) {
	if s.inTx {
		s.counts.inc(&s.counts.countQueued)
	}
	return s.Store.CountChatQueuedMessages(ctx, id)
}

func (s *countingStore) IsChatHeartbeatStale(ctx context.Context, arg database.IsChatHeartbeatStaleParams) (bool, error) {
	if s.inTx {
		s.counts.inc(&s.counts.heartbeatStale)
	}
	return s.Store.IsChatHeartbeatStale(ctx, arg)
}

// TestUpdateSingleTransitionReadsOnlyQueueCountUnderLock pins the
// round-trip budget of a transition: the lock and the queue count taken
// right after it seed validation, and the commit write feeds the publish,
// so no other chat read runs while the row lock is held. Reads creeping
// back here directly lengthen lock hold time.
func TestUpdateSingleTransitionReadsOnlyQueueCountUnderLock(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)

	counts := &roundTripCounts{}
	m := chatstate.NewChatMachine(&countingStore{Store: f.DB, counts: counts}, f.Pub, created.Chat.ID)

	committed, err := m.UpdateReturning(ctx, func(tx *chatstate.Tx, _ database.Store) error {
		_, err := tx.FinishTurn(chatstate.FinishTurnInput{})
		return err
	})
	require.NoError(t, err)

	require.Equal(t, 1, counts.lock)
	require.Equal(t, 1, counts.chatWrites, "a transition updates the chats row exactly once")
	require.Zero(t, counts.getChatByID, "GetChatByID must not run under the transition lock")
	require.Equal(t, 1, counts.countQueued, "only the post-lock queue count")
	require.Zero(t, counts.heartbeatStale, "IsChatHeartbeatStale must not run under the transition lock")

	after, err := f.DB.GetChatByID(ctx, created.Chat.ID)
	require.NoError(t, err)
	require.Equal(t, database.ChatStatusWaiting, after.Status)
	require.Equal(t, created.Chat.SnapshotVersion+1, after.SnapshotVersion)
	require.Equal(t, after, committed, "UpdateReturning hands back the committed row")
}

// TestUpdateWritesChatRowOnce pins the single-commit-write contract for
// callback shapes that have no execution-state write of their own: a
// history-only transition ends in exactly one chats UPDATE, Update's
// bump-only write. A metadata-only callback also gets exactly one commit
// write, but its own raw metadata UPDATE is a second chats write that
// Update does not fold in, so the counts here cover commit writes only.
// The row the commit write returns is the one UpdateReturning hands back.
func TestUpdateWritesChatRowOnce(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)

	counts := &roundTripCounts{}
	m := chatstate.NewChatMachine(&countingStore{Store: f.DB, counts: counts}, f.Pub, created.Chat.ID)

	committed, err := m.UpdateReturning(ctx, func(tx *chatstate.Tx, _ database.Store) error {
		msg := userTextMessage("step", f.User.ID, f.Model.ID)
		msg.Role = database.ChatMessageRoleAssistant
		_, err := tx.CommitStep(chatstate.CommitStepInput{Messages: []chatstate.Message{msg}})
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 1, counts.chatWrites, "CommitStep commits through Update's bump-only write")

	afterStep, err := f.DB.GetChatByID(ctx, created.Chat.ID)
	require.NoError(t, err)
	require.Equal(t, created.Chat.SnapshotVersion+1, afterStep.SnapshotVersion)
	require.Equal(t, afterStep.SnapshotVersion, afterStep.HistoryVersion, "the commit write records the history change")
	require.Equal(t, afterStep, committed, "the returned row carries the bump-only write's versions")
	msgs, err := f.DB.GetChatMessagesByChatID(ctx, database.GetChatMessagesByChatIDParams{ChatID: created.Chat.ID})
	require.NoError(t, err)
	require.Equal(t, afterStep.SnapshotVersion, msgs[len(msgs)-1].Revision, "messages carry the committed version")

	counts.chatWrites = 0
	require.NoError(t, m.Update(ctx, func(_ *chatstate.Tx, store database.Store) error {
		_, err := store.UpdateChatTitleByID(ctx, database.UpdateChatTitleByIDParams{ID: created.Chat.ID, Title: "renamed", TitleSource: database.ChatTitleSourceUser})
		return err
	}))
	require.Equal(t, 1, counts.chatWrites, "a metadata-only callback still gets exactly one commit write")

	afterTitle, err := f.DB.GetChatByID(ctx, created.Chat.ID)
	require.NoError(t, err)
	require.Equal(t, afterStep.SnapshotVersion+1, afterTitle.SnapshotVersion)
	require.Equal(t, afterStep.HistoryVersion, afterTitle.HistoryVersion, "no history change recorded")
}

// TestUpdateBundleRereadsAfterLockedRowConsumed proves the locked row is
// single-use: the first transition validates against it, and the second
// transition in the same callback performs a real read so it observes the
// first transition's write rather than the stale locked row.
func TestUpdateBundleRereadsAfterLockedRowConsumed(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)

	counts := &roundTripCounts{}
	m := chatstate.NewChatMachine(&countingStore{Store: f.DB, counts: counts}, f.Pub, created.Chat.ID)

	// R0 -> W (FinishTurn) -> XW (SetArchived). SetArchived is only
	// valid from W, so it must see FinishTurn's write.
	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, _ database.Store) error {
		if _, err := tx.FinishTurn(chatstate.FinishTurnInput{}); err != nil {
			return err
		}
		_, err := tx.SetArchived(chatstate.SetArchivedInput{Archived: true})
		return err
	}))

	require.Equal(t, 1, counts.lock)
	require.Equal(t, 1, counts.getChatByID, "second transition re-reads once the locked row is consumed")
	require.Equal(t, 2, counts.countQueued, "post-lock count, then the second transition's re-read")

	after, err := f.DB.GetChatByID(ctx, created.Chat.ID)
	require.NoError(t, err)
	require.True(t, after.Archived)
	require.Equal(t, database.ChatStatusWaiting, after.Status)
}

// TestCurrentDoesNotConsumeLockedRow verifies a callback can inspect
// state via Current and then run a transition without either step
// reading the chat row again. This is the worker acquisition pattern.
func TestCurrentDoesNotConsumeLockedRow(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)

	counts := &roundTripCounts{}
	m := chatstate.NewChatMachine(&countingStore{Store: f.DB, counts: counts}, f.Pub, created.Chat.ID)

	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, _ database.Store) error {
		chat, state, err := tx.Current()
		if err != nil {
			return err
		}
		require.Equal(t, created.Chat.ID, chat.ID)
		require.Equal(t, chatstate.StateR0, state)
		_, err = tx.FinishTurn(chatstate.FinishTurnInput{})
		return err
	}))

	require.Zero(t, counts.getChatByID)
	require.Equal(t, 1, counts.countQueued, "only the post-lock queue count")
}

// newAuthzStore wraps store in the dbauthz layer so tests exercise the same
// authorization pre-reads production does.
func newAuthzStore(t *testing.T, store database.Store) database.Store {
	t.Helper()
	acs := &atomic.Pointer[dbauthz.AccessControlStore]{}
	var s dbauthz.AccessControlStore = dbauthz.AGPLTemplateAccessControlStore{}
	acs.Store(&s)
	return dbauthz.New(store, rbac.NewStrictCachingAuthorizer(prometheus.NewRegistry()), slogtest.Make(t, nil), acs)
}

// TestUpdateThroughDBAuthzAuthorizesWritesFromCachedRBAC runs a transition
// through the dbauthz layer, as production does. Without the cached RBAC
// object every write's authorization re-read the chat under the lock; with
// it, only the bump's own pre-lock authorization reads the chat.
func TestUpdateThroughDBAuthzAuthorizesWritesFromCachedRBAC(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	created := createTestChat(t, f)

	counts := &roundTripCounts{}
	authz := newAuthzStore(t, &countingStore{Store: f.DB, counts: counts})
	ctx := dbauthz.AsChatd(testutil.Context(t, testutil.WaitShort))
	m := chatstate.NewChatMachine(authz, f.Pub, created.Chat.ID)

	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, _ database.Store) error {
		_, err := tx.FinishTurn(chatstate.FinishTurnInput{})
		return err
	}))

	require.Equal(t, 1, counts.lock)
	require.Equal(t, 1, counts.getChatByID,
		"only the lock's authorization reads the chat; the callback's write must not")
	require.Equal(t, 1, counts.countQueued, "only the post-lock queue count")
	require.Zero(t, counts.heartbeatStale)
}

// TestUpdateThroughDBAuthzWithCallerCachedRBACReadsNothing shows that a
// caller which already holds the chat (HTTP middleware, the worker runner)
// can cache its RBAC object up front so even the lock authorizes without a
// read: the transition then touches the chat row only through the lock and
// its commit write.
func TestUpdateThroughDBAuthzWithCallerCachedRBACReadsNothing(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	created := createTestChat(t, f)

	counts := &roundTripCounts{}
	authz := newAuthzStore(t, &countingStore{Store: f.DB, counts: counts})
	ctx := dbauthz.AsChatd(testutil.Context(t, testutil.WaitShort))
	ctx, err := dbauthz.WithChatRBAC(ctx, dbauthz.CacheableChatRBAC(created.Chat))
	require.NoError(t, err)
	m := chatstate.NewChatMachine(authz, f.Pub, created.Chat.ID)

	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, _ database.Store) error {
		_, err := tx.FinishTurn(chatstate.FinishTurnInput{})
		return err
	}))

	require.Equal(t, 1, counts.lock)
	require.Zero(t, counts.getChatByID, "no chat read at all when the caller cached the RBAC object")
}
