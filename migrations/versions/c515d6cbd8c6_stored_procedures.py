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
    "contain_principal_overflow(value NUMERIC(24))",
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

get_min_account_balance_sp = ReplaceableObject(
    "get_min_account_balance(acc account)",
    """
    RETURNS BIGINT AS $$
    DECLARE
      issuing_limit NUMERIC(24);
    BEGIN
      IF acc.creditor_id=0 THEN
        BEGIN
          issuing_limit := acc.config_data::JSON -> 'limit';
        EXCEPTION
          WHEN invalid_text_representation THEN
            NULL;
        END;

        RETURN -LEAST(
          contain_principal_overflow(
            COALESCE(issuing_limit, 9223372036854775807)
          ),
          contain_principal_overflow(
            LEAST(acc.negligible_amount, 9.999e23)::NUMERIC(24)
          )
        );
      END IF;

      RETURN 0;
    END;
    $$ LANGUAGE plpgsql;
    """
)

reject_transfer_sp = ReplaceableObject(
    "reject_transfer("
    " tr transfer_request,"
    " status_code TEXT,"
    " total_locked_amount BIGINT"
    ")",
    """
    RETURNS void AS $$
    BEGIN
      INSERT INTO rejected_transfer_signal (
        debtor_id,
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        status_code,
        total_locked_amount,
        sender_creditor_id,
        inserted_at
      )
      VALUES (
        tr.debtor_id,
        tr.coordinator_type,
        tr.coordinator_id,
        tr.coordinator_request_id,
        status_code,
        total_locked_amount,
        tr.sender_creditor_id,
        CURRENT_TIMESTAMP
      );
    END;
    $$ LANGUAGE plpgsql;
    """
)

prepare_transfer_sp = ReplaceableObject(
    "prepare_transfer("
    " tr transfer_request,"
    " INOUT sender_account account,"
    " current_ts TIMESTAMP WITH TIME ZONE,"
    " commit_period INTEGER,"
    " amount BIGINT"
    ")",
    """
    AS $$
    DECLARE
      deadline TIMESTAMP WITH TIME ZONE;
    BEGIN
      sender_account.total_locked_amount := contain_principal_overflow(
        sender_account.total_locked_amount::NUMERIC(24) + amount::NUMERIC(24)
      );
      sender_account.pending_transfers_count := (
        sender_account.pending_transfers_count + 1
      );
      sender_account.last_transfer_id := (
        sender_account.last_transfer_id + 1
      );
      deadline := LEAST(
        current_ts + make_interval(secs => commit_period),
        tr.deadline
      );

      INSERT INTO prepared_transfer (
        debtor_id, sender_creditor_id, transfer_id,
        coordinator_type, coordinator_id, coordinator_request_id,
        locked_amount, recipient_creditor_id, final_interest_rate_ts,
        demurrage_rate, deadline, prepared_at
      )
      VALUES (
        tr.debtor_id, tr.sender_creditor_id, sender_account.last_transfer_id,
        tr.coordinator_type, tr.coordinator_id, tr.coordinator_request_id,
        amount, tr.recipient_creditor_id, tr.final_interest_rate_ts,
        -50, deadline, current_ts
      );

      INSERT INTO prepared_transfer_signal (
        debtor_id, sender_creditor_id, transfer_id,
        coordinator_type, coordinator_id, coordinator_request_id,
        locked_amount, recipient_creditor_id, prepared_at,
        demurrage_rate, deadline, final_interest_rate_ts, inserted_at
      )
      VALUES (
        tr.debtor_id, tr.sender_creditor_id, sender_account.last_transfer_id,
        tr.coordinator_type, tr.coordinator_id, tr.coordinator_request_id,
        amount, tr.recipient_creditor_id, current_ts,
        -50, deadline, tr.final_interest_rate_ts, current_ts
      );
    END;
    $$ LANGUAGE plpgsql;
    """
)

process_transfer_requests_sp = ReplaceableObject(
    "process_transfer_requests(did BIGINT, cid BIGINT, commit_period INTEGER)",
    """
    RETURNS void AS $$
    DECLARE
      tr transfer_request%ROWTYPE;
      sender_account account%ROWTYPE;
      expendable_amount BIGINT;
      current_ts TIMESTAMP WITH TIME ZONE = CURRENT_TIMESTAMP;
      subnet_mask BIGINT = -0x0000010000000000;
      prepared_transfers BOOLEAN = FALSE;
    BEGIN
      FOR tr IN
        SELECT *
        FROM transfer_request
        WHERE debtor_id=did AND sender_creditor_id=cid
        FOR UPDATE SKIP LOCKED

      LOOP
        IF sender_account IS NULL THEN
          sender_account := lock_account(did, cid);
        END IF;

        DELETE FROM transfer_request
        WHERE
          debtor_id=tr.debtor_id
          AND sender_creditor_id=tr.sender_creditor_id
          AND transfer_request_id=tr.transfer_request_id;

        IF sender_account.creditor_id IS NULL THEN
          PERFORM reject_transfer(tr, 'SENDER_IS_UNREACHABLE', 0);

        ELSIF (
          tr.coordinator_type = 'agent'
          AND (
            sender_account.creditor_id & subnet_mask = 0
            OR (
               sender_account.creditor_id & subnet_mask
               != tr.recipient_creditor_id & subnet_mask
            )
          )
        ) THEN
          PERFORM reject_transfer(tr, 'RECIPIENT_IS_UNREACHABLE', 0);

        ELSIF sender_account.pending_transfers_count >= 2147483647 THEN
          PERFORM reject_transfer(tr, 'TOO_MANY_TRANSFERS', sender_account.total_locked_amount);

        ELSIF tr.sender_creditor_id = tr.recipient_creditor_id THEN
          PERFORM reject_transfer(tr, 'RECIPIENT_SAME_AS_SENDER', sender_account.total_locked_amount);

        ELSIF sender_account.last_interest_rate_change_ts > tr.final_interest_rate_ts THEN
          PERFORM reject_transfer(tr, 'NEWER_INTEREST_RATE', sender_account.total_locked_amount);

        ELSE
          expendable_amount := contain_principal_overflow(
            floor(
              calc_current_balance(
                sender_account.creditor_id,
                sender_account.principal,
                sender_account.interest,
                sender_account.interest_rate,
                sender_account.last_change_ts,
                current_ts
              )
            )::NUMERIC(24)
            - sender_account.total_locked_amount::NUMERIC(24)
            - get_min_account_balance(sender_account)::NUMERIC(24)
          );
          expendable_amount := LEAST(expendable_amount, tr.max_locked_amount);
          expendable_amount := GREATEST(0::BIGINT, expendable_amount);

          IF expendable_amount < tr.min_locked_amount THEN
            PERFORM reject_transfer(tr, 'INSUFFICIENT_AVAILABLE_AMOUNT', sender_account.total_locked_amount);
          ELSE
            sender_account := prepare_transfer(
              tr, sender_account, current_ts, commit_period, expendable_amount
            );
            prepared_transfers := TRUE;
          END IF;
        END IF;
      END LOOP;

      IF prepared_transfers THEN
        UPDATE account
        SET
          total_locked_amount=sender_account.total_locked_amount,
          pending_transfers_count=sender_account.pending_transfers_count,
          last_transfer_id=sender_account.last_transfer_id
        WHERE
          debtor_id=sender_account.debtor_id
          AND creditor_id=sender_account.creditor_id;
      END IF;
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
    op.create_sp(get_min_account_balance_sp)
    op.create_sp(reject_transfer_sp)
    op.create_sp(prepare_transfer_sp)
    op.create_sp(process_transfer_requests_sp)


def downgrade():
    op.drop_sp(calc_k_sp)
    op.drop_sp(contain_principal_overflow_sp)
    op.drop_sp(calc_current_balance_sp)
    op.drop_sp(lock_account_sp)
    op.drop_sp(insert_account_update_signal_sp)
    op.drop_sp(lock_or_create_account_sp)
    op.drop_sp(get_min_account_balance_sp)
    op.drop_sp(reject_transfer_sp)
    op.drop_sp(prepare_transfer_sp)
    op.drop_sp(process_transfer_requests_sp)
