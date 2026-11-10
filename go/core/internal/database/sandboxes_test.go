package database

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

func sandboxFixture(t *testing.T, client *Client, revision string) (*apiv1.Sandbox, SandboxCreateOptions) {
	t.Helper()
	ctx := t.Context()
	upsertErr := client.UpsertSandboxTemplateDefinition(ctx, SandboxTemplateDefinition{Namespace: "team-a", SandboxTemplateName: "scratch", SandboxTemplateUID: "uid", DesiredRevision: revision})
	require.NoError(t, upsertErr)
	require.NoError(t, recordErr(client.RecordSandboxRevision(ctx, SandboxRevision{RuntimeArtifact: RuntimeArtifact{Revision: revision, Kind: "sandbox", Namespace: "team-a", ActorTemplateAtespace: "team-a", ActorTemplateName: revision, ActorTemplateUID: "actor-uid"},
		SandboxTemplateName: "scratch", SandboxTemplateUID: "uid", SourceSnapshot: []byte(`{}`)}, RecordRevisionOptions{Ready: true})))
	hash := sha256.Sum256([]byte("request"))
	return &apiv1.Sandbox{Id: uuid.NewString(), Creator: "alice", SandboxTemplate: &apiv1.ResourceReference{Namespace: "team-a", Name: "scratch"}, Name: "experiment"},
		SandboxCreateOptions{RequestHash: hash[:], TemplateUID: "uid", TTL: time.Hour}
}

func TestRecordReadySandboxRevisionOnlyCurrentActiveTemplate(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	request, options := sandboxFixture(t, client, "first")
	definition := SandboxTemplateDefinition{Namespace: "team-a", SandboxTemplateName: "scratch", SandboxTemplateUID: "uid", DesiredRevision: "second"}
	upsertErr := client.UpsertSandboxTemplateDefinition(ctx, definition)
	require.NoError(t, upsertErr)
	revision, err := client.GetSandboxRevision(ctx, "first")
	require.NoError(t, err)
	revision.Revision, revision.ActorTemplateName = "second", "second"
	require.NoError(t, recordErr(client.RecordSandboxRevision(ctx, *revision, RecordRevisionOptions{})))
	assertAvailable := func(want string) {
		t.Helper()
		input := proto.CloneOf(request)
		input.Id = uuid.NewString()
		instance, _, err := client.CreateSandbox(ctx, input, uuid.NewString(), options)
		if want == "" {
			require.ErrorIs(t, err, ErrNotFound)
			return
		}
		require.NoError(t, err)
		require.Equal(t, want, instance.PreparedRevision)
	}
	assertAvailable("first")
	require.NoError(t, recordErr(client.RecordSandboxRevision(ctx, *revision, RecordRevisionOptions{Ready: true})))
	assertAvailable("second")
	revision.Revision, revision.ActorTemplateName = "first", "first"
	require.NoError(t, recordErr(client.RecordSandboxRevision(ctx, *revision, RecordRevisionOptions{Ready: true})))
	assertAvailable("second")
	require.NoError(t, client.RetireSandboxTemplateIdentities(ctx, "team-a", "scratch"))
	require.NoError(t, recordErr(client.RecordSandboxRevision(ctx, *revision, RecordRevisionOptions{Ready: true})))
	assertAvailable("")
	upsertErr = client.UpsertSandboxTemplateDefinition(ctx, definition)
	require.NoError(t, upsertErr)
	assertAvailable("second")
	revision.Revision, revision.ActorTemplateName = "unused", "unused"
	require.NoError(t, recordErr(client.RecordSandboxRevision(ctx, *revision, RecordRevisionOptions{})))
	claimed, err := client.BeginRuntimeRevisionDeletion(ctx, "unused")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.ErrorIs(t, recordErr(client.RecordSandboxRevision(ctx, *revision, RecordRevisionOptions{Ready: true})), ErrObjectDeleting)
}

func TestRecordReadySandboxRevisionResetsGoldenRetry(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	sandboxFixture(t, client, "rev")
	revision, err := client.GetSandboxRevision(ctx, "rev")
	require.NoError(t, err)
	failedAt := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	record := func(uid string, options RecordRevisionOptions) GoldenRetry {
		t.Helper()
		revision.ActorTemplateUID = uid
		retry, err := client.RecordSandboxRevision(ctx, *revision, options)
		require.NoError(t, err)
		retry.FailedAt = retry.FailedAt.UTC()
		return retry
	}

	require.Equal(t, GoldenRetry{Retries: 1, FailedAt: failedAt, New: true}, record("uid-1", RecordRevisionOptions{GoldenFailedAt: failedAt}))
	require.Equal(t, GoldenRetry{Retries: 1}, record("uid-2", RecordRevisionOptions{}), "a rebuild that is not ready yet keeps its retry count")
	require.Equal(t, GoldenRetry{Retries: 2, FailedAt: failedAt, New: true}, record("uid-2", RecordRevisionOptions{GoldenFailedAt: failedAt}))
	require.Equal(t, GoldenRetry{}, record("uid-3", RecordRevisionOptions{Ready: true}), "a ready sandbox revision resets its retry state")
	require.Equal(t, GoldenRetry{Retries: 1, FailedAt: failedAt, New: true}, record("uid-4", RecordRevisionOptions{GoldenFailedAt: failedAt}))
}

func finishSandbox(t *testing.T, client *Client, id string, kind apiv1.RuntimeOperation) *apiv1.Sandbox {
	t.Helper()
	op, err := client.BeginSandboxOperation(t.Context(), id, kind)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSandboxOperation(t.Context(), id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	result, err := client.FinishSandboxOperation(t.Context(), id, op.ID, executor, "")
	require.NoError(t, err)
	return result
}

func TestSandboxCreationPinsRetriesAndRetainsTombstones(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "revision-1")
	original := proto.CloneOf(input)
	created, wasCreated, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	require.True(t, wasCreated)
	require.True(t, proto.Equal(original, input), "creation must not mutate the caller's protobuf")
	require.Equal(t, "experiment", created.Name)
	require.Equal(t, time.Hour, created.ExpiresAt.AsTime().Sub(created.CreatedAt.AsTime()))
	ready := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, ready.State)
	_, _ = sandboxFixture(t, client, "revision-2")
	options.TTL = 2 * time.Hour
	retried, wasCreated, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	require.False(t, wasCreated)
	require.Equal(t, "revision-1", retried.PreparedRevision)
	require.Equal(t, created.ExpiresAt, retried.ExpiresAt)
	options.RequestHash = make([]byte, 32)
	_, _, err = client.CreateSandbox(ctx, input, "create", options)
	require.ErrorIs(t, err, ErrIdempotencyConflict)
	hash := sha256.Sum256([]byte("request"))
	options.RequestHash = hash[:]
	require.NoError(t, client.RetireSandboxTemplateIdentities(ctx, "team-a", "scratch"))
	_, _, err = client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	_, err = client.GetSandbox(ctx, created.Id, "mallory")
	require.ErrorIs(t, err, ErrNotFound)
	deleted := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.State)
	_, _, err = client.CreateSandbox(ctx, input, "create", options)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	garbage, err := client.BeginRuntimeRevisionDeletion(ctx, "revision-1")
	require.NoError(t, err)
	require.NotNil(t, garbage)
}

func TestSandboxConcurrentCreation(t *testing.T) {
	const requests = 12
	for _, tc := range []struct {
		name       string
		distinct   bool
		conflicts  bool
		wantCount  int
		wantErrors int
	}{
		{name: "duplicate requests", wantCount: 1},
		{name: "distinct requests exceed former owner limit", distinct: true, wantCount: requests},
		{name: "conflicting retries", conflicts: true, wantCount: 1, wantErrors: requests / 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient(setupTestDB(t), "public")
			input, options := sandboxFixture(t, client, "revision")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			type result struct {
				instance *apiv1.Sandbox
				created  bool
				err      error
			}
			start := make(chan struct{})
			results := make(chan result, requests)
			for i := range requests {
				request := proto.CloneOf(input)
				request.Id = uuid.NewString()
				requestID := "create"
				if tc.distinct {
					requestID = uuid.NewString()
				}
				options := options
				if tc.conflicts && i%2 == 0 {
					hash := sha256.Sum256([]byte("different request"))
					options.RequestHash = hash[:]
				}
				go func() {
					<-start
					instance, created, err := client.CreateSandbox(ctx, request, requestID, options)
					results <- result{instance: instance, created: created, err: err}
				}()
			}
			close(start)
			var first *apiv1.Sandbox
			created, conflicts := 0, 0
			for range requests {
				result := <-results
				if result.err != nil {
					require.ErrorIs(t, result.err, ErrIdempotencyConflict)
					require.False(t, result.created)
					conflicts++
					continue
				}
				if result.created {
					created++
				}
				if first == nil {
					first = result.instance
				} else if !tc.distinct {
					require.True(t, proto.Equal(first, result.instance), "all matching retries must return the winning resource")
				}
			}
			require.Equal(t, tc.wantCount, created)
			require.Equal(t, tc.wantErrors, conflicts)
			instances, err := client.ListSandboxes(ctx, input.Creator, "", 100, nil)
			require.NoError(t, err)
			require.Len(t, instances, tc.wantCount)
		})
	}
}

func TestSandboxExpirationPaginationAndFailureGeneration(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "revision")
	live, _, err := client.CreateSandbox(ctx, input, "live", options)
	require.NoError(t, err)
	pending, err := client.ListExpiredSandboxes(ctx, "", 100)
	require.NoError(t, err)
	require.Empty(t, pending, "pending lifecycle work is not background work")
	options.TTL = time.Second
	var ids []string
	for range 3 {
		input.Id = uuid.NewString()
		instance, _, err := client.CreateSandbox(ctx, input, uuid.NewString(), options)
		require.NoError(t, err)
		ids = append(ids, instance.Id)
	}
	require.Eventually(t, func() bool {
		expired, err := client.ListExpiredSandboxes(ctx, "", 100)
		return err == nil && len(expired) == 3
	}, 3*time.Second, 10*time.Millisecond)
	first, err := client.ListExpiredSandboxes(ctx, "", 2)
	require.NoError(t, err)
	require.Len(t, first, 2)
	second, err := client.ListExpiredSandboxes(ctx, first[1], 2)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.ElementsMatch(t, ids, append(first, second...))
	op, err := client.BeginSandboxOperation(ctx, live.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	require.NoError(t, client.RecordSandboxOperationFailure(ctx, live.Id, op.ID))
	failed, err := client.GetSandbox(ctx, live.Id, "alice")
	require.NoError(t, err)
	require.Equal(t, "RuntimeUnavailable", failed.Failure.Reason)
	completed := finishSandbox(t, client, live.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, client.RecordSandboxOperationFailure(ctx, live.Id, op.ID))
	ready, err := client.GetSandbox(ctx, live.Id, "alice")
	require.NoError(t, err)
	require.Nil(t, ready.Failure, "late failure cannot overwrite successful completion")
	require.True(t, proto.Equal(completed, ready), "late failure must not change the stored payload or update timestamp")
}

func TestSandboxLifecycleSupersedesEndedAttempts(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "revision")
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	suspend, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSandboxOperation(ctx, created.Id, suspend.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	_, err = client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.ErrorIs(t, err, ErrConflict, "active execution must finish before changing intent")
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, created.Id, suspend.ID, executor))
	resume, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.NoError(t, err)
	require.NotEqual(t, suspend.ID, resume.ID)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME, resume.Instance.Operation,
		"the recorded READY state must not hide an in-flight suspension")
	_, err = client.FinishSandboxOperation(ctx, created.Id, suspend.ID, executor, "")
	require.ErrorIs(t, err, ErrConflict)
	require.NoError(t, client.RecordSandboxOperationFailure(ctx, created.Id, suspend.ID))
	current, err := client.GetSandboxOperation(ctx, created.Id)
	require.NoError(t, err)
	require.Equal(t, resume.ID, current.ID)
	require.Nil(t, current.Instance.Failure)
	executor = uuid.New()
	claimed, err = client.ClaimSandboxOperation(ctx, created.Id, resume.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, created.Id, resume.ID, executor))
	deletion, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.NoError(t, err)
	require.NotEqual(t, resume.ID, deletion.ID)
	_, err = client.FinishSandboxOperation(ctx, created.Id, resume.ID, executor, "")
	require.ErrorIs(t, err, ErrConflict)
	_, err = client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.ErrorIs(t, err, ErrFailedPrecondition, "deletion must not be canceled into leaked compute")
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
}

func TestRetainedSandboxSurvivesSuspendedExpiration(t *testing.T) {
	// Given a retained sandbox whose original active deadline passes while suspended.
	pool := setupTestDB(t)
	client := NewClient(pool, "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "retained")
	input.RetainWhileSuspended = true
	options.TTL = time.Second
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	suspended := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	require.Nil(t, suspended.ExpiresAt)
	waitForSandboxDeadline(t, created)

	// When a fresh client loads it and expiration attempts deletion.
	reopened := NewClient(pool, "public")
	saved, err := reopened.GetSandbox(ctx, created.Id, input.Creator)
	require.NoError(t, err)
	expired, err := reopened.ListExpiredSandboxes(ctx, "", 100)
	require.NoError(t, err)
	_, err = reopened.BeginExpiredSandboxDeletion(ctx, created.Id)

	// Assert that the workspace and its revision remain retained without a deadline.
	require.ErrorIs(t, err, ErrFailedPrecondition)
	require.True(t, saved.RetainWhileSuspended)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED, saved.State)
	require.Nil(t, saved.ExpiresAt)
	require.NotContains(t, expired, created.Id)
	require.NoError(t, reopened.RetireSandboxTemplateIdentities(ctx, "team-a", "scratch"))
	garbage, err := reopened.BeginRuntimeRevisionDeletion(ctx, "retained")
	require.NoError(t, err)
	require.Nil(t, garbage)
	deleted := finishSandbox(t, reopened, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.State)
	garbage, err = reopened.BeginRuntimeRevisionDeletion(ctx, "retained")
	require.NoError(t, err)
	require.NotNil(t, garbage, "explicit deletion releases the retained revision")
}

func TestRetainedSandboxResumeRenewsOnlyOnce(t *testing.T) {
	// Given a retained suspension past its original, fractional active TTL.
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "retained")
	input.RetainWhileSuspended = true
	options.TTL = 1250*time.Millisecond + 123*time.Microsecond
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	waitForSandboxDeadline(t, created)

	// A creation retry cannot alter the stored active TTL or retention policy.
	options.TTL = time.Hour
	input.RetainWhileSuspended = false
	retried, wasCreated, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	require.False(t, wasCreated)
	require.True(t, retried.RetainWhileSuspended)
	require.Nil(t, retried.ExpiresAt)

	// When concurrent callers resume it, every caller observes the same admission.
	const requests = 8
	type response struct {
		operation *SandboxOperation
		err       error
	}
	start := make(chan struct{})
	responses := make(chan response, requests)
	before := time.Now()
	for range requests {
		go func() {
			<-start
			op, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
			responses <- response{operation: op, err: err}
		}()
	}
	close(start)
	var first *SandboxOperation
	for range requests {
		result := <-responses
		require.NoError(t, result.err)
		if first == nil {
			first = result.operation
		}
		require.Equal(t, first.ID, result.operation.ID)
		require.True(t, proto.Equal(first.Instance.ExpiresAt, result.operation.Instance.ExpiresAt))
	}

	// Assert that only the original bounded duration was restored, including its fraction.
	deadline := first.Instance.ExpiresAt.AsTime()
	require.GreaterOrEqual(t, deadline.Sub(before), 1250*time.Millisecond)
	require.LessOrEqual(t, time.Until(deadline), 1250*time.Millisecond+124*time.Microsecond)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME, first.Instance.Operation)
	ready := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.True(t, proto.Equal(first.Instance.ExpiresAt, ready.ExpiresAt))
	duplicate, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE, duplicate.Instance.Operation)
	require.True(t, proto.Equal(first.Instance.ExpiresAt, duplicate.Instance.ExpiresAt), "a ready sandbox does not renew on duplicate resume")

	// A later completed pause starts a distinct active period on its next resume.
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	second, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
	require.True(t, second.Instance.ExpiresAt.AsTime().After(deadline))
}

func TestRetainedSandboxPendingResumeStillExpires(t *testing.T) {
	// Given a retained sandbox with an abandoned resume attempt.
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "retained")
	input.RetainWhileSuspended = true
	options.TTL = time.Second
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	resume, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSandboxOperation(ctx, created.Id, resume.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, created.Id, resume.ID, executor))
	waitForSandboxDeadline(t, resume.Instance)

	// When expiration deletes it, a retry cannot reset its deadline or revive it.
	expired, err := client.ListExpiredSandboxes(ctx, "", 100)
	require.NoError(t, err)
	require.Contains(t, expired, created.Id)
	_, err = client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	deletion, err := client.BeginExpiredSandboxDeletion(ctx, created.Id)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE, deletion.Instance.Operation)
	require.True(t, proto.Equal(resume.Instance.ExpiresAt, deletion.Instance.ExpiresAt))
	_, err = client.FinishSandboxOperation(ctx, created.Id, resume.ID, executor, "")
	require.ErrorIs(t, err, ErrConflict)
	_, err = client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	deleted := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.State)
}

func TestSandboxExpirationRechecksCompletedSuspensionAndResume(t *testing.T) {
	// Given an expiration candidate whose suspension has not yet completed.
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "retained")
	input.RetainWhileSuspended = true
	options.TTL = time.Second
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	suspend, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSandboxOperation(ctx, created.Id, suspend.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	waitForSandboxDeadline(t, created)
	expired, err := client.ListExpiredSandboxes(ctx, "", 100)
	require.NoError(t, err)
	require.Contains(t, expired, created.Id)

	// When suspension finishes before the selected candidate is deleted.
	suspended, err := client.FinishSandboxOperation(ctx, created.Id, suspend.ID, executor, "")
	require.NoError(t, err)
	require.Nil(t, suspended.ExpiresAt)
	_, err = client.BeginExpiredSandboxDeletion(ctx, created.Id)
	require.ErrorIs(t, err, ErrFailedPrecondition, "stale selection cannot delete a completed retained suspension")
	resume, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.NoError(t, err)
	_, err = client.BeginExpiredSandboxDeletion(ctx, created.Id)

	// Assert that the same stale selection also cannot delete its new active period.
	require.ErrorIs(t, err, ErrFailedPrecondition)
	current, err := client.GetSandboxOperation(ctx, created.Id)
	require.NoError(t, err)
	require.Equal(t, resume.ID, current.ID)
	require.True(t, proto.Equal(resume.Instance.ExpiresAt, current.Instance.ExpiresAt))
}

func TestSandboxExpirationWinsBeforeSuspensionCompletes(t *testing.T) {
	// Given an expired pending suspension whose attempt has ended.
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "retained")
	input.RetainWhileSuspended = true
	options.TTL = time.Second
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	suspend, err := client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSandboxOperation(ctx, created.Id, suspend.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, created.Id, suspend.ID, executor))
	waitForSandboxDeadline(t, created)

	// When expiration admits deletion before a late suspension completion.
	deletion, err := client.BeginExpiredSandboxDeletion(ctx, created.Id)
	require.NoError(t, err)
	_, err = client.FinishSandboxOperation(ctx, created.Id, suspend.ID, executor, "")

	// Assert that stale completion cannot turn the deleting workspace into a retained one.
	require.ErrorIs(t, err, ErrConflict)
	require.NotEqual(t, suspend.ID, deletion.ID)
	_, err = client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
}

func TestDefaultSandboxSuspensionKeepsFixedExpiration(t *testing.T) {
	// Given a sandbox created without retention and then suspended.
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	input, options := sandboxFixture(t, client, "ordinary")
	options.TTL = time.Second
	created, _, err := client.CreateSandbox(ctx, input, "create", options)
	require.NoError(t, err)
	finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	suspended := finishSandbox(t, client, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	require.False(t, suspended.RetainWhileSuspended)
	require.True(t, proto.Equal(created.ExpiresAt, suspended.ExpiresAt))
	waitForSandboxDeadline(t, created)

	// When the original deadline passes, ordinary expiration still applies.
	expired, err := client.ListExpiredSandboxes(ctx, "", 100)
	require.NoError(t, err)
	require.Contains(t, expired, created.Id)
	_, err = client.BeginSandboxOperation(ctx, created.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.ErrorIs(t, err, ErrFailedPrecondition)
	deletion, err := client.BeginExpiredSandboxDeletion(ctx, created.Id)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE, deletion.Instance.Operation)
}

func TestSandboxRetentionPreservesLegacyViewReaders(t *testing.T) {
	// Given the migrated schema and a sandbox created by the new controller.
	client := NewClient(setupTestDB(t), "public")
	input, options := sandboxFixture(t, client, "retained")
	input.RetainWhileSuspended = true
	created, _, err := client.CreateSandbox(t.Context(), input, "create", options)
	require.NoError(t, err)

	// When the previous controller's exact SELECT * projection is decoded with
	// its strict pgx row type, additive storage fields must not appear in the view.
	type legacySandboxRow struct {
		runtimeInstanceRow
		Data                []byte
		RequestID           string
		Namespace           string
		SandboxTemplateName string
		RevisionReceipt     string
		RequestHash         []byte
		ExpiresAt           time.Time
	}
	legacy, err := queryOne(t.Context(), client.db,
		`SELECT * FROM sandbox_record WHERE id = $1 AND user_id = $2`,
		pgx.RowToStructByName[legacySandboxRow], created.Id, created.Creator)

	// Assert that a mixed-version reader still loads the existing sandbox shape.
	require.NoError(t, err)
	require.Equal(t, created.Id, legacy.ID.String())
	require.Equal(t, created.Creator, legacy.UserID)
	require.Equal(t, "retained", legacy.RevisionReceipt)
	require.Equal(t, "create", legacy.RequestID)
	require.True(t, created.ExpiresAt.AsTime().Equal(legacy.ExpiresAt))
}

func waitForSandboxDeadline(t *testing.T, instance *apiv1.Sandbox) {
	t.Helper()
	require.NotNil(t, instance.ExpiresAt)
	require.Eventually(t, func() bool {
		return time.Now().After(instance.ExpiresAt.AsTime())
	}, 3*time.Second, 10*time.Millisecond)
}
