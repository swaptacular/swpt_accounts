"""stored procedures

Revision ID: c515d6cbd8c6
Revises: 3655736a38df
Create Date: 2024-08-06 14:21:10.376602

"""
from alembic import op
import sqlalchemy as sa

from swpt_accounts.migration_helpers import ReplaceableObject

# revision identifiers, used by Alembic.
revision = 'c515d6cbd8c6'
down_revision = '3655736a38df'
branch_labels = None
depends_on = None

calc_k_sp = ReplaceableObject(
    "calc_k(interest_rate FLOAT)",
    """
    RETURNS FLOAT AS $$
    BEGIN
      RETURN ln(1 + interest_rate / 100) / 31557600;
    END;
    $$ LANGUAGE plpgsql;
    """
)

contain_principal_overflow_sp = ReplaceableObject(
    "contain_principal_overflow(value NUMERIC(32))",
    """
    RETURNS BIGINT AS $$
    DECLARE
      min_value value%TYPE = -9223372036854775807;
      max_value value%TYPE = 9223372036854775807;
    BEGIN
      IF value < min_value THEN
        RETURN min_value::BIGINT;
      ELSIF value > max_value THEN
        RETURN max_value::BIGINT;
      ELSE
        RETURN value::BIGINT;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

calc_current_balance_sp = ReplaceableObject(
    "calc_current_balance("
    " creditor_id BIGINT,"
    " principal BIGINT,"
    " interest FLOAT,"
    " interest_rate FLOAT,"
    " last_change_ts TIMESTAMP WITH TIME ZONE,"
    " current_ts TIMESTAMP WITH TIME ZONE"
    ")",
    """
    RETURNS NUMERIC(32,8) AS $$
    DECLARE
      current_balance NUMERIC(32,8) = principal;
      k FLOAT;
      passed_seconds FLOAT;
    BEGIN
      IF creditor_id != 0 THEN
        current_balance := current_balance + interest;

        IF current_balance > 0 THEN
          k := calc_k(interest_rate);
          passed_seconds := GREATEST(
             0::FLOAT,
             (
               EXTRACT(EPOCH FROM current_ts)
               - EXTRACT(EPOCH FROM last_change_ts)
             )::FLOAT
          );
          current_balance := current_balance * exp(k * passed_seconds);
        END IF;
      END IF;

      RETURN current_balance;
    END;
    $$ LANGUAGE plpgsql;
    """
)

lock_account_sp = ReplaceableObject(
    "lock_account(did BIGINT, cid BIGINT)",
    """
    RETURNS account AS $$
    DECLARE
      acc account%ROWTYPE;
    BEGIN
      SELECT * INTO acc
      FROM account
      WHERE
        debtor_id=did
        AND creditor_id=cid
        AND status_flags & 1 = 0
      FOR UPDATE;

      RETURN acc;
    END;
    $$ LANGUAGE plpgsql;
    """
)


insert_account_update_signal_sp = ReplaceableObject(
    "insert_account_update_signal("
    " INOUT acc account,"
    " current_ts TIMESTAMP WITH TIME ZONE"
    ")",
    """
    AS $$
    BEGIN
      acc.last_heartbeat_ts := current_ts;
      acc.pending_account_update := FALSE;

      INSERT INTO account_update_signal (
         debtor_id, creditor_id, last_change_seqnum,
         last_change_ts, principal, interest,
         interest_rate, last_interest_rate_change_ts,
         last_transfer_number, last_transfer_committed_at,
         last_config_ts, last_config_seqnum, creation_date,
         negligible_amount, config_data, config_flags,
         debtor_info_iri, debtor_info_content_type,
         debtor_info_sha256, inserted_at
      )
      VALUES (
         acc.debtor_id, acc.creditor_id, acc.last_change_seqnum,
         acc.last_change_ts, acc.principal, acc.interest,
         acc.interest_rate, acc.last_interest_rate_change_ts,
         acc.last_transfer_number, acc.last_transfer_committed_at,
         acc.last_config_ts, acc.last_config_seqnum, acc.creation_date,
         acc.negligible_amount, acc.config_data, acc.config_flags,
         acc.debtor_info_iri, acc.debtor_info_content_type,
         acc.debtor_info_sha256, acc.last_change_ts
      );
    END;
    $$ LANGUAGE plpgsql;
    """
)

lock_or_create_account_sp = ReplaceableObject(
    "lock_or_create_account("
    " did BIGINT,"
    " cid BIGINT,"
    " current_ts TIMESTAMP WITH TIME ZONE"
    ")",
    """
    RETURNS account AS $$
    DECLARE
      acc account%ROWTYPE;
      t0 TIMESTAMP WITH TIME ZONE;
    BEGIN
      SELECT * INTO acc
      FROM account
      WHERE debtor_id=did AND creditor_id=cid
      FOR UPDATE;

      IF NOT FOUND THEN
        t0 := to_timestamp(0);

        BEGIN
          INSERT INTO account (
            debtor_id, creditor_id, creation_date,
            last_change_seqnum, last_change_ts, principal,
            interest_rate, interest, last_interest_rate_change_ts,
            last_config_ts, last_config_seqnum, last_transfer_number,
            last_transfer_committed_at, negligible_amount, config_flags,
            config_data, status_flags, total_locked_amount,
            pending_transfers_count, last_transfer_id, previous_interest_rate,
            last_heartbeat_ts, last_interest_capitalization_ts,
            last_deletion_attempt_ts, pending_account_update
          )
          VALUES (
             did, cid, current_ts::date,
             0, current_ts, 0,
             0, 0, t0,
             t0, 0, 0,
             t0, 0, 0,
             '', 0, 0,
             0, (current_ts::date - t0::date)::BIGINT << 40, 0,
             current_ts, t0,
             t0, FALSE
          )
          RETURNING * INTO acc;
        EXCEPTION
          WHEN unique_violation THEN
            RAISE serialization_failure
            USING MESSAGE = 'account creation race condition';
        END;

        acc := insert_account_update_signal(acc, current_ts);
      END IF;

      IF acc.status_flags & 1 != 0 THEN
        acc.status_flags := acc.status_flags & ~(1::INTEGER);
        acc.last_change_seqnum := CASE
          WHEN acc.last_change_seqnum = 2147483647 THEN -2147483648
          ELSE acc.last_change_seqnum + 1
        END;
        acc.last_change_ts := GREATEST(acc.last_change_ts, current_ts);

        acc := insert_account_update_signal(acc, current_ts);
      END IF;

      RETURN acc;
    END;
    $$ LANGUAGE plpgsql;
    """
)


def upgrade():
    op.create_sp(calc_k_sp)
    op.create_sp(contain_principal_overflow_sp)
    op.create_sp(calc_current_balance_sp)
    op.create_sp(lock_account_sp)
    op.create_sp(insert_account_update_signal_sp)
    op.create_sp(lock_or_create_account_sp)


def downgrade():
    op.drop_sp(calc_k_sp)
    op.drop_sp(contain_principal_overflow_sp)
    op.drop_sp(calc_current_balance_sp)
    op.drop_sp(lock_account_sp)
    op.drop_sp(insert_account_update_signal_sp)
    op.drop_sp(lock_or_create_account_sp)
