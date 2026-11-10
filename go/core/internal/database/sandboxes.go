package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

type sandboxRow struct {
	runtimeInstanceRow
	Data                 []byte
	RequestID            string
	Namespace            string
	SandboxTemplateName  string
	RevisionReceipt      string
	RequestHash          []byte
	ExpiresAt            time.Time
	RetainWhileSuspended bool
	ActiveTTLNS          int64
}

func (r sandboxRow) sandbox() (*apiv1.Sandbox, error) {
	s := &apiv1.Sandbox{}
	if err := proto.Unmarshal(r.Data, s); err != nil {
		return nil, fmt.Errorf("decode Sandbox %s: %w", r.ID, err)
	}
	state, operation, err := r.lifecycle()
	if err != nil {
		return nil, err
	}
	// Relational columns own identity, lifecycle, filtering, and expiration.
	s.Id, s.Creator = r.ID.String(), r.UserID
	s.State, s.Operation = state, operation
	s.PreparedRevision, s.ExpiresAt = r.RevisionReceipt, timestamppb.New(r.ExpiresAt)
	s.RetainWhileSuspended = r.RetainWhileSuspended
	if r.retainedSuspension() {
		s.ExpiresAt = nil
	}
	if s.SandboxTemplate == nil {
		s.SandboxTemplate = &apiv1.ResourceReference{}
	}
	s.SandboxTemplate.Namespace, s.SandboxTemplate.Name = r.Namespace, r.SandboxTemplateName
	return s, nil
}

// retainedSuspension identifies a completed, opted-in suspension. Pending work
// keeps its active deadline so an abandoned resume can still expire.
func (r sandboxRow) retainedSuspension() bool {
	return r.RetainWhileSuspended && r.State == apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED.String() &&
		r.Operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE.String()
}

// saveSandboxPayload preserves the full protobuf, including unknown fields.
// Callers hold the runtime row lock and commit lifecycle columns with this write.
func saveSandboxPayload(ctx context.Context, tx pgx.Tx, instance *apiv1.Sandbox) ([]byte, error) {
	data, err := proto.Marshal(instance)
	if err != nil {
		return nil, fmt.Errorf("encode Sandbox %s: %w", instance.Id, err)
	}
	return data, execSQL(ctx, tx, `UPDATE sandbox SET data = $2 WHERE id = $1`, instance.Id, data)
}

// SandboxCreateOptions contains creation inputs that are not part of the resource.
type SandboxCreateOptions struct {
	RequestHash []byte
	TemplateUID string
	TTL         time.Duration
}

// CreateSandbox atomically reserves a sandbox and its latest successful revision,
// or returns ErrNotFound if none is available. A duplicate creator/requestID
// returns the original resource when inputs match.
// The boolean reports whether this call created the sandbox.
func (c *Client) CreateSandbox(ctx context.Context, request *apiv1.Sandbox, requestID string, options SandboxCreateOptions) (*apiv1.Sandbox, bool, error) {
	existing, err := readSandboxRequest(ctx, c.db, request.GetCreator(), requestID)
	if err == nil {
		if !bytes.Equal(existing.RequestHash, options.RequestHash) {
			return nil, false, ErrIdempotencyConflict
		}
		if existing.State == "RUNTIME_STATE_DELETED" {
			return nil, false, ErrFailedPrecondition
		}
		instance, err := existing.sandbox()
		return instance, false, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("get Sandbox request: %w", err)
	}

	var row sandboxRow
	err = c.withTx(ctx, func(tx pgx.Tx) error {
		row, err = insertSandbox(ctx, tx, request, requestID, options)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		existing, err = readSandboxRequest(ctx, c.db, request.GetCreator(), requestID)
		if err != nil {
			return nil, false, fmt.Errorf("get concurrent Sandbox request: %w", err)
		}
		if !bytes.Equal(existing.RequestHash, options.RequestHash) {
			return nil, false, ErrIdempotencyConflict
		}
		if existing.State == "RUNTIME_STATE_DELETED" {
			return nil, false, ErrFailedPrecondition
		}
		instance, err := existing.sandbox()
		return instance, false, err
	}
	if err != nil {
		return nil, false, fmt.Errorf("insert Sandbox: %w", err)
	}
	instance, err := row.sandbox()
	return instance, err == nil, err
}

func readSandboxRequest(ctx context.Context, db dbExecutor, userID, requestID string) (sandboxRow, error) {
	return queryOne(ctx, db, `
		SELECT r.*, s.retain_while_suspended, s.active_ttl_ns
		FROM sandbox_record r JOIN sandbox s USING (id)
		WHERE r.user_id = $1 AND r.request_id = $2
	`, pgx.RowToStructByName[sandboxRow], userID, requestID)
}

// insertSandbox reserves a new sandbox in CREATING/CREATE and pins the active
// template's latest successful revision. A missing prepared
// target returns ErrNotFound. A duplicate creator/requestID returns
// pgx.ErrNoRows so the caller can roll back and read the winning transaction.
func insertSandbox(ctx context.Context, tx pgx.Tx, request *apiv1.Sandbox, requestID string, options SandboxCreateOptions) (sandboxRow, error) {
	if options.TTL < time.Second || options.TTL > 24*time.Hour {
		return sandboxRow{}, ErrFailedPrecondition
	}
	var revision string
	var createdAt time.Time
	err := tx.QueryRow(ctx, `
		SELECT latest_successful_revision, clock_timestamp() FROM sandbox_template_definition
		WHERE namespace = $1 AND sandbox_template_name = $2 AND sandbox_template_uid = $3
		    AND retired_at IS NULL AND latest_successful_revision IS NOT NULL
		FOR UPDATE
	`, request.GetSandboxTemplate().GetNamespace(), request.GetSandboxTemplate().GetName(), options.TemplateUID).Scan(&revision, &createdAt)
	if err != nil {
		return sandboxRow{}, notFoundOr(err)
	}
	if err := lockRuntimeRevisionForReference(ctx, tx, revision, runtimeKindSandbox); err != nil {
		return sandboxRow{}, err
	}
	instance := proto.CloneOf(request)
	instance.PreparedRevision = revision
	instance.State = apiv1.RuntimeState_RUNTIME_STATE_CREATING
	instance.Operation = apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE
	instance.CreatedAt = timestamppb.New(createdAt)
	instance.UpdatedAt = timestamppb.New(createdAt)
	instance.ExpiresAt = timestamppb.New(createdAt.Add(options.TTL))
	data, err := proto.Marshal(instance)
	if err != nil {
		return sandboxRow{}, fmt.Errorf("encode Sandbox %s: %w", instance.Id, err)
	}
	if _, err := queryOne(ctx, tx, `
		INSERT INTO runtime_instance (id, kind, user_id, request_id, prepared_revision, state, operation)
		VALUES ($1, 'sandbox', $2, $3, $4, 'RUNTIME_STATE_CREATING', 'RUNTIME_OPERATION_CREATE')
		ON CONFLICT (kind, user_id, request_id) DO NOTHING RETURNING id
	`, pgx.RowTo[uuid.UUID], instance.Id, instance.Creator, requestID, revision); err != nil {
		return sandboxRow{}, err
	}
	if err := execSQL(ctx, tx, `
		INSERT INTO sandbox (id, namespace, sandbox_template_name, revision_receipt, request_hash, expires_at, data,
		    retain_while_suspended, active_ttl_ns)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, instance.Id, instance.GetSandboxTemplate().GetNamespace(), instance.GetSandboxTemplate().GetName(), revision, options.RequestHash, instance.ExpiresAt.AsTime(), data,
		instance.GetRetainWhileSuspended(), int64(options.TTL)); err != nil {
		return sandboxRow{}, err
	}
	return readSandbox(ctx, tx, instance.Id)
}

// GetSandbox includes deletion tombstones so authorized retries can observe cleanup.
func (c *Client) GetSandbox(ctx context.Context, id, userID string) (*apiv1.Sandbox, error) {
	row, err := queryOne(ctx, c.db, `
		SELECT r.*, s.retain_while_suspended, s.active_ttl_ns
		FROM sandbox_record r JOIN sandbox s USING (id)
		WHERE r.id = $1 AND r.user_id = $2
	`, pgx.RowToStructByName[sandboxRow], id, userID)
	if err != nil {
		return nil, notFoundOr(err)
	}
	return row.sandbox()
}

func (c *Client) ListSandboxes(ctx context.Context, userID, afterID string, limit int, template *apiv1.ResourceReference) ([]*apiv1.Sandbox, error) {
	rows, err := queryMany(ctx, c.db, `
		SELECT r.*, s.retain_while_suspended, s.active_ttl_ns
		FROM sandbox_record r JOIN sandbox s USING (id)
		WHERE r.user_id = $1 AND r.state <> 'RUNTIME_STATE_DELETED'
		    AND ($2::text = '' OR r.id > NULLIF($2, '')::uuid)
		    AND ($4::text = '' OR (r.namespace = $4 AND r.sandbox_template_name = $5)) ORDER BY r.id LIMIT $3
	`, pgx.RowToStructByName[sandboxRow], userID, afterID, limit, template.GetNamespace(), template.GetName())
	if err != nil {
		return nil, err
	}
	result := make([]*apiv1.Sandbox, 0, len(rows))
	for _, row := range rows {
		s, err := row.sandbox()
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, nil
}

func readSandbox(ctx context.Context, db dbExecutor, id string) (sandboxRow, error) {
	return queryOne(ctx, db, `
		SELECT r.*, s.retain_while_suspended, s.active_ttl_ns
		FROM sandbox_record r JOIN sandbox s USING (id) WHERE r.id = $1
	`, pgx.RowToStructByName[sandboxRow], id)
}

func lockSandbox(ctx context.Context, tx pgx.Tx, id string) (sandboxRow, error) {
	return queryOne(ctx, tx, `
		SELECT r.*, s.retain_while_suspended, s.active_ttl_ns
		FROM sandbox_record r JOIN sandbox s USING (id) WHERE r.id = $1 FOR UPDATE
	`, pgx.RowToStructByName[sandboxRow], id)
}

type SandboxOperation = RuntimeOperation[*apiv1.Sandbox]

// BeginSandboxOperation admits lifecycle work under the sandbox lock. A new
// resume of a retained suspension starts its original active TTL once; retries
// retain that deadline. Explicit deletion is allowed regardless of expiration.
func (c *Client) BeginSandboxOperation(ctx context.Context, id string, kind apiv1.RuntimeOperation) (*SandboxOperation, error) {
	var result *SandboxOperation
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSandbox(ctx, tx, id)
		if err != nil {
			return notFoundOr(err)
		}
		result, err = admitSandboxOperation(ctx, tx, row, kind)
		return err
	})
	return result, err
}

// BeginExpiredSandboxDeletion rechecks an expiration candidate under its row
// lock before admitting deletion. A resumed, retained, or deleted sandbox
// returns ErrFailedPrecondition; a missing sandbox returns ErrNotFound.
func (c *Client) BeginExpiredSandboxDeletion(ctx context.Context, id string) (*SandboxOperation, error) {
	var result *SandboxOperation
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSandbox(ctx, tx, id)
		if err != nil {
			return notFoundOr(err)
		}
		expired, err := sandboxExpired(ctx, tx, row)
		if err != nil {
			return err
		}
		if !expired {
			return ErrFailedPrecondition
		}
		result, err = admitSandboxOperation(ctx, tx, row, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
		return err
	})
	return result, err
}

// sandboxExpired evaluates the current deadline for a locked sandbox. Only a
// completed retained suspension and a deletion tombstone have no deadline.
func sandboxExpired(ctx context.Context, tx pgx.Tx, row sandboxRow) (bool, error) {
	if row.retainedSuspension() || row.State == apiv1.RuntimeState_RUNTIME_STATE_DELETED.String() {
		return false, nil
	}
	return queryOne(ctx, tx, `SELECT expires_at <= clock_timestamp() FROM sandbox WHERE id = $1`, pgx.RowTo[bool], row.ID)
}

// admitSandboxOperation shares lifecycle admission for explicit requests and
// the expiration worker. The caller holds the sandbox lock and transaction.
func admitSandboxOperation(ctx context.Context, tx pgx.Tx, row sandboxRow, kind apiv1.RuntimeOperation) (*SandboxOperation, error) {
	if kind != apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE {
		expired, err := sandboxExpired(ctx, tx, row)
		if err != nil {
			return nil, err
		}
		if expired {
			return nil, ErrFailedPrecondition
		}
	}
	needed, err := sandboxOperationNeeded(row.runtimeInstanceRow, kind)
	if err != nil {
		return nil, err
	}
	if needed {
		if row.ExecutorID != nil {
			active, err := queryOne(ctx, tx, `SELECT executor_expires_at > clock_timestamp() FROM runtime_instance WHERE id = $1`, pgx.RowTo[bool], row.ID)
			if err != nil {
				return nil, err
			}
			if active {
				return nil, ErrConflict
			}
		}
		if kind == apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME && row.retainedSuspension() {
			row.ExpiresAt, err = queryOne(ctx, tx, `
				UPDATE sandbox SET expires_at = clock_timestamp() + ($2::bigint / 1000) * interval '1 microsecond'
				WHERE id = $1 RETURNING expires_at
			`, pgx.RowTo[time.Time], row.ID, row.ActiveTTLNS)
			if err != nil {
				return nil, err
			}
		}
		row, err = beginSandboxOperation(ctx, tx, row, kind)
		if err != nil {
			return nil, err
		}
	}
	instance, err := row.sandbox()
	if err != nil {
		return nil, err
	}
	result := runtimeOperationFor(row.runtimeInstanceRow, instance)
	return &result, nil
}

// Sandbox lifecycle requests may supersede pending work after its active attempt
// ends. Operation IDs reject stale completions.
func sandboxOperationNeeded(row runtimeInstanceRow, requested apiv1.RuntimeOperation) (bool, error) {
	state, operation, err := row.lifecycle()
	if err != nil {
		return false, err
	}
	if requested < apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE || requested > apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE {
		return false, ErrFailedPrecondition
	}
	if state == apiv1.RuntimeState_RUNTIME_STATE_DELETED {
		if requested == apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE {
			return false, nil
		}
		return false, ErrNotFound
	}
	if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE && requested != operation {
		return false, ErrFailedPrecondition
	}
	if requested == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && state != apiv1.RuntimeState_RUNTIME_STATE_CREATING {
		return false, nil
	}
	if state == apiv1.RuntimeState_RUNTIME_STATE_CREATING && requested != apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && requested != apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE {
		return false, ErrFailedPrecondition
	}
	if operation != apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE {
		return operation != requested || row.OperationID == nil, nil
	}
	switch requested {
	case apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND:
		return state != apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED, nil
	case apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME:
		return state != apiv1.RuntimeState_RUNTIME_STATE_READY, nil
	default:
		return true, nil
	}
}

func beginSandboxOperation(ctx context.Context, tx pgx.Tx, row sandboxRow, kind apiv1.RuntimeOperation) (sandboxRow, error) {
	var err error
	row.runtimeInstanceRow, err = beginRuntimeOperation(ctx, tx, row.runtimeInstanceRow, runtimeKindSandbox, kind)
	if err != nil {
		return row, err
	}
	instance, err := row.sandbox()
	if err != nil {
		return row, err
	}
	instance.UpdatedAt = timestamppb.Now()
	row.Data, err = saveSandboxPayload(ctx, tx, instance)
	return row, err
}

func (c *Client) ClaimSandboxOperation(ctx context.Context, id string, operationID, executorID uuid.UUID) (bool, error) {
	return c.claimRuntimeOperation(ctx, id, runtimeKindSandbox, operationID, executorID)
}

func (c *Client) FinishSandboxOperation(ctx context.Context, id string, operationID, executorID uuid.UUID, failure string) (*apiv1.Sandbox, error) {
	var result *apiv1.Sandbox
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSandbox(ctx, tx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		next, err := completeRuntimeOperation(row.runtimeInstanceRow, operationID, executorID, failure)
		if err != nil {
			return err
		}
		if err := saveRuntimeLifecycle(ctx, tx, next, runtimeKindSandbox); err != nil {
			return err
		}
		row.runtimeInstanceRow = next
		result, err = row.sandbox()
		if err != nil {
			return err
		}
		result.UpdatedAt = timestamppb.Now()
		result.Failure = nil
		if failure != "" {
			result.Failure = &apiv1.Failure{Reason: "PreparationFailed", Message: failure}
		}
		_, err = saveSandboxPayload(ctx, tx, result)
		return err
	})
	return result, err
}

func (c *Client) GetSandboxOperation(ctx context.Context, id string) (*SandboxOperation, error) {
	row, err := readSandbox(ctx, c.db, id)
	if err != nil {
		return nil, notFoundOr(err)
	}
	instance, err := row.sandbox()
	if err != nil {
		return nil, err
	}
	result := runtimeOperationFor(row.runtimeInstanceRow, instance)
	return &result, nil
}

func (c *Client) RecordSandboxOperationFailure(ctx context.Context, id string, operationID uuid.UUID) error {
	return c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSandbox(ctx, tx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if row.OperationID == nil || *row.OperationID != operationID || row.Operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE.String() {
			return nil
		}
		instance, err := row.sandbox()
		if err != nil {
			return err
		}
		instance.Failure = &apiv1.Failure{Reason: "RuntimeUnavailable", Message: "Runtime operation is pending; retry the lifecycle request"}
		instance.UpdatedAt = timestamppb.Now()
		_, err = saveSandboxPayload(ctx, tx, instance)
		return err
	})
}

// ListExpiredSandboxes returns expiration candidates ordered by ID. A completed
// retained suspension is omitted; callers must recheck eligibility when deleting.
func (c *Client) ListExpiredSandboxes(ctx context.Context, afterID string, limit int) ([]string, error) {
	return queryMany(ctx, c.db, `SELECT s.id::text FROM sandbox s JOIN runtime_instance r USING (id)
	    WHERE r.kind = 'sandbox' AND r.state <> 'RUNTIME_STATE_DELETED'
	        AND s.expires_at <= clock_timestamp()
	        AND NOT (s.retain_while_suspended AND r.state = 'RUNTIME_STATE_SUSPENDED' AND r.operation = 'RUNTIME_OPERATION_NONE')
	        AND (NULLIF($1, '')::uuid IS NULL OR s.id > NULLIF($1, '')::uuid)
	    ORDER BY s.id LIMIT $2`, pgx.RowTo[string], afterID, limit)
}
