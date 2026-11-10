-- +goose Up
ALTER TABLE sandbox
    ADD COLUMN retain_while_suspended BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN active_ttl_ns BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT sandbox_active_ttl_bounds CHECK (
        active_ttl_ns BETWEEN 0 AND 86400000000000
        AND (NOT retain_while_suspended OR active_ttl_ns >= 1000000000)
    );

-- Keep sandbox_record's existing projection: older controllers scan SELECT *
-- into a strict row type. New readers join the retention columns explicitly.

-- +goose Down
ALTER TABLE sandbox
    DROP CONSTRAINT sandbox_active_ttl_bounds,
    DROP COLUMN active_ttl_ns,
    DROP COLUMN retain_while_suspended;
