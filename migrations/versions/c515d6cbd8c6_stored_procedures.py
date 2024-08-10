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
      RETURN ln(1 + interest_rate / 100) / 31557600;  -- seconds in an year
    END;
    $$ LANGUAGE plpgsql;
    """
)

contain_principal_overflow_sp = ReplaceableObject(
    "contain_principal_overflow(value NUMERIC(24))",
    """
    RETURNS BIGINT AS $$
    DECLARE
      min_value value%TYPE = -0x7fffffffffffffff;
      max_value value%TYPE = 0x7fffffffffffffff;
    BEGIN
      IF value < min_value THEN
        RETURN min_value;
      ELSIF value > max_value THEN
        RETURN max_value;
      ELSE
        RETURN value;
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
    BEGIN
      IF creditor_id != 0 THEN
        BEGIN
          current_balance := current_balance + interest::NUMERIC(32,8);
        EXCEPTION
          WHEN numeric_value_out_of_range THEN
            current_balance := sign(interest) * 9.999e23::NUMERIC(32,8);
        END;

        IF current_balance > 0 THEN
          BEGIN
            current_balance := current_balance * exp(
              calc_k(interest_rate)
              * GREATEST(
                0::FLOAT,
                (
                  EXTRACT(EPOCH FROM current_ts)
                  - EXTRACT(EPOCH FROM last_change_ts)
                )::FLOAT
              )
            )::NUMERIC;
          EXCEPTION
            WHEN numeric_value_out_of_range THEN
              current_balance := 9.999e23::NUMERIC(32,8);
          END;
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

      UPDATE account
      SET
        creation_date=acc.creation_date,
        last_change_seqnum=acc.last_change_seqnum,
        last_change_ts=acc.last_change_ts,
        principal=acc.principal,
        interest=acc.interest,
        last_transfer_number=acc.last_transfer_number,
        last_transfer_committed_at=acc.last_transfer_committed_at,
        status_flags=acc.status_flags,
        total_locked_amount=acc.total_locked_amount,
        pending_transfers_count=acc.pending_transfers_count,
        last_transfer_id=acc.last_transfer_id,
        last_heartbeat_ts=acc.last_heartbeat_ts,
        pending_account_update=acc.pending_account_update
      WHERE debtor_id=acc.debtor_id AND creditor_id=acc.creditor_id;

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
          WHEN acc.last_change_seqnum = 0x7fffffff THEN -0x80000000
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
          issuing_limit := CAST(
            acc.config_data::JSON ->> 'limit' AS NUMERIC(24)
          );
        EXCEPTION
          WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            NULL;
        END;

        RETURN -LEAST(
          contain_principal_overflow(
            COALESCE(issuing_limit, 9.999e23::NUMERIC(24))
          ),
          contain_principal_overflow(
            LEAST(acc.negligible_amount, 9.999e23::REAL)::NUMERIC(24)
          )
        );
      ELSE
        RETURN 0;
      END IF;
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
        debtor_id, coordinator_type, coordinator_id,
        coordinator_request_id, status_code, total_locked_amount,
        sender_creditor_id, inserted_at
      )
      VALUES (
        tr.debtor_id, tr.coordinator_type, tr.coordinator_id,
        tr.coordinator_request_id, status_code, total_locked_amount,
        tr.sender_creditor_id, CURRENT_TIMESTAMP
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
    " amount_to_lock BIGINT"
    ")",
    """
    AS $$
    DECLARE
      deadline TIMESTAMP WITH TIME ZONE;
    BEGIN
      sender_account.total_locked_amount := contain_principal_overflow(
        sender_account.total_locked_amount::NUMERIC(24)
        + amount_to_lock::NUMERIC(24)
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
        amount_to_lock, tr.recipient_creditor_id, tr.final_interest_rate_ts,
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
        amount_to_lock, tr.recipient_creditor_id, current_ts,
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
      amount_to_lock BIGINT;
      subnet_mask BIGINT = -0x0000010000000000;  -- This is 0xffffff0000000000
      had_prepared_transfers BOOLEAN = FALSE;
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
          PERFORM reject_transfer(
            tr, 'SENDER_IS_UNREACHABLE', 0
          );
        ELSIF tr.coordinator_type = 'agent'
            AND (
              sender_account.creditor_id & subnet_mask = 0
              OR (
                 sender_account.creditor_id & subnet_mask
                 != tr.recipient_creditor_id & subnet_mask
              )
            ) THEN
          PERFORM reject_transfer(
            tr, 'RECIPIENT_IS_UNREACHABLE', 0
          );
        ELSIF sender_account.pending_transfers_count >= 0x7fffffff THEN
          PERFORM reject_transfer(
            tr, 'TOO_MANY_TRANSFERS', sender_account.total_locked_amount
          );
        ELSIF tr.sender_creditor_id = tr.recipient_creditor_id THEN
          PERFORM reject_transfer(
            tr, 'RECIPIENT_SAME_AS_SENDER', sender_account.total_locked_amount
          );
        ELSIF sender_account.last_interest_rate_change_ts
            > tr.final_interest_rate_ts THEN
          PERFORM reject_transfer(
            tr, 'NEWER_INTEREST_RATE', sender_account.total_locked_amount
          );
        ELSE
          amount_to_lock := GREATEST(
            0::BIGINT,
            LEAST(
              tr.max_locked_amount,
              contain_principal_overflow(
                floor(
                  calc_current_balance(
                    sender_account.creditor_id,
                    sender_account.principal,
                    sender_account.interest,
                    sender_account.interest_rate,
                    sender_account.last_change_ts,
                    CURRENT_TIMESTAMP
                  )
                )::NUMERIC(24)
                - sender_account.total_locked_amount::NUMERIC(24)
                - get_min_account_balance(sender_account)::NUMERIC(24)
              )
            )
          );
          IF amount_to_lock < tr.min_locked_amount THEN
            PERFORM reject_transfer(
              tr, 'INSUFFICIENT_AVAILABLE_AMOUNT',
              sender_account.total_locked_amount
            );
          ELSE
            sender_account := prepare_transfer(
              tr, sender_account, CURRENT_TIMESTAMP,
              commit_period, amount_to_lock
            );
            had_prepared_transfers := TRUE;
          END IF;
        END IF;
      END LOOP;

      IF had_prepared_transfers THEN
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

apply_account_change_sp = ReplaceableObject(
    "apply_account_change("
    " INOUT acc account,"
    " principal_delta NUMERIC(24),"
    " interest_delta FLOAT,"
    " current_ts TIMESTAMP WITH TIME ZONE"
    ")",
    """
    AS $$
    DECLARE
      new_principal NUMERIC(24) = acc.principal::NUMERIC(24) + principal_delta;
    BEGIN
      acc.interest := (
        calc_current_balance(
          acc.creditor_id,
          acc.principal,
          acc.interest,
          acc.interest_rate,
          acc.last_change_ts,
          current_ts
        )::FLOAT
        - acc.principal::FLOAT
        + interest_delta
      );

      acc.principal := contain_principal_overflow(new_principal);
      IF acc.principal != new_principal THEN
         acc.status_flags := acc.status_flags | 0b10;  -- set an overflow flag
      END IF;

      acc.last_change_seqnum := CASE
        WHEN acc.last_change_seqnum = 0x7fffffff THEN -0x80000000
        ELSE acc.last_change_seqnum + 1
      END;
      acc.last_change_ts := GREATEST(acc.last_change_ts, current_ts);
      acc.pending_account_update := TRUE;

      UPDATE account
      SET
        creation_date=acc.creation_date,
        last_change_seqnum=acc.last_change_seqnum,
        last_change_ts=acc.last_change_ts,
        principal=acc.principal,
        interest=acc.interest,
        last_transfer_number=acc.last_transfer_number,
        last_transfer_committed_at=acc.last_transfer_committed_at,
        status_flags=acc.status_flags,
        total_locked_amount=acc.total_locked_amount,
        pending_transfers_count=acc.pending_transfers_count,
        last_transfer_id=acc.last_transfer_id,
        last_heartbeat_ts=acc.last_heartbeat_ts,
        pending_account_update=acc.pending_account_update
      WHERE debtor_id=acc.debtor_id AND creditor_id=acc.creditor_id;
    END;
    $$ LANGUAGE plpgsql;
    """
)

calc_status_code_sp = ReplaceableObject(
    "calc_status_code("
    " pt prepared_transfer,"
    " committed_amount BIGINT,"
    " expendable_amount NUMERIC(24),"
    " last_interest_rate_change_ts TIMESTAMP WITH TIME ZONE,"
    " current_ts TIMESTAMP WITH TIME ZONE"
    ")",
    """
    RETURNS TEXT AS $$
    DECLARE
      status_code TEXT;
    BEGIN
      IF committed_amount > 0 THEN
        IF current_ts > pt.deadline THEN
          status_code := 'TIMEOUT';

        ELSIF last_interest_rate_change_ts > pt.final_interest_rate_ts THEN
          status_code := 'NEWER_INTEREST_RATE';

        ELSIF
            NOT (
              -- The expendable amount is big enough.
              committed_amount <= (
                expendable_amount + pt.locked_amount::NUMERIC(24)
              )
              OR (
                -- The locked amount is big enough.
                committed_amount <= pt.locked_amount
                AND (
                  pt.sender_creditor_id = 0
                  OR committed_amount::FLOAT <= pt.locked_amount * exp(
                    calc_k(pt.demurrage_rate)
                    * GREATEST(
                      0::FLOAT,
                      (
                        EXTRACT(EPOCH FROM current_ts)
                        - EXTRACT(EPOCH FROM pt.prepared_at)
                      )::FLOAT
                    )
                  )
                )
              )
            ) THEN
          status_code := 'INSUFFICIENT_AVAILABLE_AMOUNT';
        END IF;
      END IF;

      RETURN COALESCE(status_code, 'OK');
    END;
    $$ LANGUAGE plpgsql;
    """
)

finalization_request_pair_type = ReplaceableObject(
    "finalization_request_pair",
    """
    AS (
      fr finalization_request,
      pt prepared_transfer
    )
    """
)

insert_account_transfer_signal_sp = ReplaceableObject(
    "insert_account_transfer_signal("
    " INOUT acc account,"
    " coordinator_type TEXT,"
    " other_creditor_id BIGINT,"
    " committed_at TIMESTAMP WITH TIME ZONE,"
    " acquired_amount BIGINT,"
    " transfer_note_format TEXT,"
    " transfer_note TEXT,"
    " principal BIGINT"
    ")",
    """
    AS $$
    DECLARE
      previous_transfer_number BIGINT;
    BEGIN
      IF acc.creditor_id != 0
          AND NOT (
              coordinator_type != 'agent'
              AND 0 < acquired_amount
              AND acquired_amount::FLOAT <= acc.negligible_amount
          ) THEN
        previous_transfer_number := acc.last_transfer_number;
        acc.last_transfer_number := previous_transfer_number + 1;
        acc.last_transfer_committed_at = committed_at;

        INSERT INTO account_transfer_signal (
          debtor_id, creditor_id, transfer_number,
          coordinator_type, other_creditor_id, committed_at,
          acquired_amount, transfer_note_format, transfer_note,
          creation_date, principal, previous_transfer_number,
          inserted_at
        )
        VALUES (
          acc.debtor_id, acc.creditor_id, acc.last_transfer_number,
          coordinator_type, other_creditor_id, committed_at,
          acquired_amount, transfer_note_format, transfer_note,
          acc.creation_date, principal, previous_transfer_number,
          CURRENT_TIMESTAMP
        );
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

process_finalization_requests_sp = ReplaceableObject(
    "process_finalization_requests("
    " did BIGINT,"
    " sender_cid BIGINT,"
    " ignore_all BOOLEAN"
    ")",
    """
    RETURNS void AS $$
    DECLARE
      principal_delta NUMERIC(24) = 0;
      decreased_pending_transfers_count BOOLEAN = FALSE;
      pair finalization_request_pair%ROWTYPE;
      current_fr finalization_request%ROWTYPE;
      current_pt prepared_transfer%ROWTYPE;
      sender_account account%ROWTYPE;
      starting_balance NUMERIC(24);
      min_account_balance NUMERIC(24);
      status_code TEXT;
      committed_amount BIGINT;
    BEGIN
      FOR pair IN
        SELECT fr, pt
        FROM
          finalization_request fr
          LEFT OUTER JOIN prepared_transfer pt ON (
            pt.debtor_id = fr.debtor_id
            AND pt.sender_creditor_id = fr.sender_creditor_id
            AND pt.transfer_id = fr.transfer_id
            AND pt.coordinator_type = fr.coordinator_type
            AND pt.coordinator_id = fr.coordinator_id
            AND pt.coordinator_request_id = fr.coordinator_request_id
          )
        WHERE fr.debtor_id = did AND fr.sender_creditor_id = sender_cid
        FOR UPDATE OF fr SKIP LOCKED

      LOOP
        current_fr = pair.fr;
        current_pt = pair.pt;

        DELETE FROM finalization_request
        WHERE
          debtor_id = current_fr.debtor_id
          AND sender_creditor_id = current_fr.sender_creditor_id
          AND transfer_id = current_fr.transfer_id;

        IF sender_account IS NULL THEN
          sender_account := lock_account(did, sender_cid);

          IF sender_account.creditor_id IS NOT NULL THEN
            starting_balance := calc_current_balance(
              sender_account.creditor_id,
              sender_account.principal,
              sender_account.interest,
              sender_account.interest_rate,
              sender_account.last_change_ts,
              CURRENT_TIMESTAMP
            );
            min_account_balance := get_min_account_balance(sender_account);
          END IF;
        END IF;

        IF (
            sender_account.creditor_id IS NOT NULL
            AND current_pt.transfer_id IS NOT NULL
            AND NOT ignore_all
            ) THEN
          status_code := calc_status_code(
              current_pt,
              current_fr.committed_amount,
              (
                + starting_balance
                + principal_delta
                - sender_account.total_locked_amount::NUMERIC(24)
                - min_account_balance
              ),
              sender_account.last_interest_rate_change_ts,
              CURRENT_TIMESTAMP
          );
          committed_amount := CASE WHEN status_code = 'OK'
            THEN current_fr.committed_amount
            ELSE 0
          END;
          principal_delta := principal_delta - committed_amount::NUMERIC(24);

          DELETE FROM prepared_transfer
          WHERE
            debtor_id = current_pt.debtor_id
            AND sender_creditor_id = current_pt.sender_creditor_id
            AND transfer_id = current_pt.transfer_id;

          sender_account.total_locked_amount := GREATEST(
              0::BIGINT,
              sender_account.total_locked_amount - current_pt.locked_amount
          );
          sender_account.pending_transfers_count := GREATEST(
              0::INTEGER,
              sender_account.pending_transfers_count - 1
          );
          decreased_pending_transfers_count := TRUE;

          INSERT INTO finalized_transfer_signal (
            debtor_id, sender_creditor_id,
            transfer_id, coordinator_type,
            coordinator_id, coordinator_request_id,
            prepared_at, finalized_at,
            committed_amount, total_locked_amount,
            status_code, inserted_at
          )
          VALUES (
            current_pt.debtor_id, current_pt.sender_creditor_id,
            current_pt.transfer_id, current_pt.coordinator_type,
            current_pt.coordinator_id, current_pt.coordinator_request_id,
            current_pt.prepared_at, CURRENT_TIMESTAMP,
            committed_amount, sender_account.total_locked_amount,
            status_code, CURRENT_TIMESTAMP
          );

          IF committed_amount > 0 THEN
            sender_account := insert_account_transfer_signal(
                sender_account,
                current_pt.coordinator_type,
                current_pt.recipient_creditor_id,
                CURRENT_TIMESTAMP,
                -committed_amount,
                current_fr.transfer_note_format,
                current_fr.transfer_note,
                contain_principal_overflow(
                  sender_account.principal::NUMERIC(24)
                  - committed_amount::NUMERIC(24)
                )
            );
            INSERT INTO pending_balance_change_signal (
              debtor_id, other_creditor_id,
              creditor_id, committed_at,
              coordinator_type, transfer_note_format,
              transfer_note, principal_delta, inserted_at
            )
            VALUES (
              current_pt.debtor_id, current_pt.sender_creditor_id,
              current_pt.recipient_creditor_id, CURRENT_TIMESTAMP,
              current_pt.coordinator_type, current_fr.transfer_note_format,
              current_fr.transfer_note, committed_amount, CURRENT_TIMESTAMP
            );
          END IF;
        END IF;
      END LOOP;

      IF principal_delta != 0 THEN
        PERFORM apply_account_change(
          sender_account, principal_delta, 0, CURRENT_TIMESTAMP
        );
      ELSIF decreased_pending_transfers_count THEN
        UPDATE account
        SET
          total_locked_amount=sender_account.total_locked_amount,
          pending_transfers_count=sender_account.pending_transfers_count
        WHERE
          debtor_id=sender_account.debtor_id
          AND creditor_id=sender_account.creditor_id;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)


def upgrade():
    op.create_type(finalization_request_pair_type)
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
    op.create_sp(apply_account_change_sp)
    op.create_sp(calc_status_code_sp)
    op.create_sp(process_finalization_requests_sp)
    op.create_sp(insert_account_transfer_signal_sp)


def downgrade():
    op.drop_sp(insert_account_transfer_signal_sp)
    op.drop_sp(process_finalization_requests_sp)
    op.drop_sp(calc_status_code_sp)
    op.drop_sp(apply_account_change_sp)
    op.drop_sp(process_transfer_requests_sp)
    op.drop_sp(prepare_transfer_sp)
    op.drop_sp(reject_transfer_sp)
    op.drop_sp(get_min_account_balance_sp)
    op.drop_sp(lock_or_create_account_sp)
    op.drop_sp(insert_account_update_signal_sp)
    op.drop_sp(lock_account_sp)
    op.drop_sp(calc_current_balance_sp)
    op.drop_sp(contain_principal_overflow_sp)
    op.drop_sp(calc_k_sp)
    op.drop_type(finalization_request_pair_type)
