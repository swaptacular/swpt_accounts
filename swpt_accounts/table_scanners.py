import math
from base64 import b16encode
from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from sqlalchemy import insert, update, select, delete
from sqlalchemy.sql.expression import true, tuple_, or_
from sqlalchemy.orm import load_only
from flask import current_app
from swpt_accounts.extensions import db, chores_publisher
from swpt_accounts.models import (
    Account,
    AccountUpdateSignal,
    AccountPurgeSignal,
    PreparedTransfer,
    PreparedTransferSignal,
    RegisteredBalanceChange,
    ROOT_CREDITOR_ID,
    calc_current_balance,
    is_negligible_balance,
    contain_principal_overflow,
    is_valid_account,
)
from swpt_accounts.fetch_api_client import get_root_config_data_dict
from swpt_accounts.chores import create_chore_message

INSERT_BATCH_SIZE = 5000


class AccountScanner(TableScanner):
    """Sends account heartbeat signals, purge deleted accounts."""

    table = Account.__table__
    pk = tuple_(Account.debtor_id, Account.creditor_id)
    columns = [
        Account.debtor_id,
        Account.creditor_id,
        Account.status_flags,
        Account.last_change_ts,
        Account.creation_date,
        Account.last_heartbeat_ts,
        Account.pending_account_update,
        Account.last_deletion_attempt_ts,
        Account.config_flags,
        Account.negligible_amount,
        Account.principal,
        Account.interest,
        Account.interest_rate,
        Account.last_interest_capitalization_ts,
        Account.last_interest_rate_change_ts,
        Account.debtor_info_iri,
        Account.debtor_info_content_type,
        Account.debtor_info_sha256,
    ]

    def __init__(self):
        super().__init__()
        message_max_delay = timedelta(
            days=current_app.config["APP_MESSAGE_MAX_DELAY_DAYS"]
        )
        account_heartbeat_interval = timedelta(
            days=current_app.config["APP_ACCOUNT_HEARTBEAT_DAYS"]
        )

        self.account_purge_delay = (
            timedelta(
                days=current_app.config["APP_INTRANET_EXTREME_DELAY_DAYS"]
            )
            + timedelta(
                days=current_app.config["APP_PREPARED_TRANSFER_MAX_DELAY_DAYS"]
            )
            + timedelta(days=2)
        )
        self.few_days_interval = timedelta(days=3)
        self.deletion_attempts_min_interval = timedelta(
            days=current_app.config["APP_DELETION_ATTEMPTS_MIN_DAYS"]
        )
        self.interest_rate_change_min_interval = (
            message_max_delay + timedelta(days=1)
        )
        self.max_interest_to_principal_ratio = current_app.config[
            "APP_MAX_INTEREST_TO_PRINCIPAL_RATIO"
        ]
        self.min_interest_cap_interval = timedelta(
            days=current_app.config["APP_MIN_INTEREST_CAPITALIZATION_DAYS"]
        )

        # To prevent clogging the signal bus with heartbeat signals,
        # we ensure that the account heartbeat interval is not shorter
        # than the allowed delay in the signal bus.
        self.account_heartbeat_interval = max(
            account_heartbeat_interval, message_max_delay
        )

        assert self.max_interest_to_principal_ratio > 0.0

    @property
    def blocks_per_query(self) -> int:
        return current_app.config["APP_ACCOUNTS_SCAN_BLOCKS_PER_QUERY"]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config["APP_ACCOUNTS_SCAN_BEAT_MILLISECS"]

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_accounts(rows, current_ts)
        self._purge_accounts(rows, current_ts)
        self._send_heartbeats(rows, current_ts)
        self._delete_accounts(rows, current_ts)
        self._capitalize_interests(rows, current_ts)
        self._change_debtor_settings(rows, current_ts)
        db.session.close()

    def _delete_parent_shard_accounts(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id

        def belongs_to_parent_shard(row) -> bool:
            return not is_valid_account(
                row[c_debtor_id], row[c_creditor_id]
            ) and is_valid_account(
                row[c_debtor_id], row[c_creditor_id], match_parent=True
            )

        pks_to_delete = [
            (row[c_debtor_id], row[c_creditor_id])
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            chosen = Account.choose_rows(pks_to_delete)
            to_delete = (
                Account.query
                .options(load_only(Account.creditor_id))
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .all()
            )

            for account in to_delete:
                db.session.delete(account)

            db.session.commit()

    def _purge_accounts(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_status_flags = c.status_flags
        c_last_change_ts = c.last_change_ts
        c_creation_date = c.creation_date
        deleted_flag = Account.STATUS_DELETED_FLAG
        date_few_days_ago = (current_ts - self.few_days_interval).date()
        purge_cutoff_ts = current_ts - self.account_purge_delay

        # If an account is created, deleted, purged, and re-created in
        # a single day, the `creation_date` of the new account will be
        # the same as the `creation_date` of the old account. We need
        # to make sure this never happens.
        pks_to_purge = [
            (row[c_debtor_id], row[c_creditor_id])
            for row in rows
            if (
                row[c_status_flags] & deleted_flag
                and row[c_last_change_ts] < purge_cutoff_ts
                and row[c_creation_date] < date_few_days_ago
                and is_valid_account(row[c_debtor_id], row[c_creditor_id])
            )
        ]

        if pks_to_purge:
            chosen = Account.choose_rows(pks_to_purge)
            to_purge = (
                Account.query
                .options(load_only(Account.creation_date))
                .join(chosen, self.pk == tuple_(*chosen.c))
                .filter(
                    Account.status_flags.op("&")(deleted_flag) == deleted_flag,
                    Account.last_change_ts < purge_cutoff_ts,
                    Account.creation_date < date_few_days_ago,
                )
                .with_for_update(skip_locked=True)
                .all()
            )

            if to_purge:
                to_insert = []
                for account in to_purge:
                    to_insert.append(
                        {
                            "debtor_id": account.debtor_id,
                            "creditor_id": account.creditor_id,
                            "creation_date": account.creation_date,
                        }
                    )
                    db.session.delete(account)

                db.session.execute(
                    insert(AccountPurgeSignal)
                    .execution_options(
                        insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                        synchronize_session=False,
                    ),
                    to_insert,
                )

            db.session.commit()

    def _send_heartbeats(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_status_flags = c.status_flags
        c_last_heartbeat_ts = c.last_heartbeat_ts
        c_pending_account_update = c.pending_account_update
        deleted_flag = Account.STATUS_DELETED_FLAG
        heartbeat_cutoff_ts = current_ts - self.account_heartbeat_interval

        pks_to_heartbeat = [
            (row[c_debtor_id], row[c_creditor_id])
            for row in rows
            if (
                not row[c_status_flags] & deleted_flag
                and (
                    row[c_last_heartbeat_ts] < heartbeat_cutoff_ts
                    or row[c_pending_account_update]
                )
                and is_valid_account(row[c_debtor_id], row[c_creditor_id])
            )
        ]

        if pks_to_heartbeat:
            chosen = Account.choose_rows(pks_to_heartbeat)
            to_heartbeat = (
                Account.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .filter(
                    Account.status_flags.op("&")(deleted_flag) == 0,
                    or_(
                        Account.last_heartbeat_ts < heartbeat_cutoff_ts,
                        Account.pending_account_update == true(),
                    ),
                )
                .with_for_update(skip_locked=True, key_share=True)
                .all()
            )

            if to_heartbeat:
                pks_to_remind = [
                    (account.debtor_id, account.creditor_id)
                    for account in to_heartbeat
                ]
                to_update = Account.choose_rows(pks_to_remind)
                db.session.execute(
                    update(Account)
                    .execution_options(synchronize_session=False)
                    .where(self.pk == tuple_(*to_update.c))
                    .values(
                        last_heartbeat_ts=current_ts,
                        pending_account_update=False,
                    )
                )

                db.session.execute(
                    insert(AccountUpdateSignal)
                    .execution_options(
                        insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                        synchronize_session=False,
                    ),
                    [
                        dict(
                            debtor_id=account.debtor_id,
                            creditor_id=account.creditor_id,
                            last_change_seqnum=account.last_change_seqnum,
                            last_change_ts=account.last_change_ts,
                            principal=account.principal,
                            interest=account.interest,
                            interest_rate=account.interest_rate,
                            last_interest_rate_change_ts=(
                                account.last_interest_rate_change_ts
                            ),
                            last_transfer_number=account.last_transfer_number,
                            last_transfer_committed_at=(
                                account.last_transfer_committed_at
                            ),
                            last_config_ts=account.last_config_ts,
                            last_config_seqnum=account.last_config_seqnum,
                            creation_date=account.creation_date,
                            negligible_amount=account.negligible_amount,
                            config_data=account.config_data,
                            config_flags=account.config_flags,
                            debtor_info_iri=account.debtor_info_iri,
                            debtor_info_content_type=(
                                account.debtor_info_content_type
                            ),
                            debtor_info_sha256=account.debtor_info_sha256,
                            inserted_at=max(
                                current_ts, account.last_change_ts
                            ),
                        )
                        for account in to_heartbeat
                    ],
                )

            db.session.commit()

    def _delete_accounts(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_last_deletion_attempt_ts = c.last_deletion_attempt_ts
        c_status_flags = c.status_flags
        c_config_flags = c.config_flags
        c_negligible_amount = c.negligible_amount
        c_principal = c.principal
        c_interest = c.interest
        c_interest_rate = c.interest_rate
        c_last_change_ts = c.last_change_ts
        scheduled_for_deletion_flag = (
            Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        )
        deleted_flag = Account.STATUS_DELETED_FLAG
        cutoff_ts = current_ts - self.deletion_attempts_min_interval
        chores = []

        for row in rows:
            creditor_id = row[c_creditor_id]
            should_be_deleted = False
            should_be_deleted_if_balance_is_negligible = (
                row[c_last_deletion_attempt_ts] <= cutoff_ts
                and row[c_config_flags] & scheduled_for_deletion_flag
                and not row[c_status_flags] & deleted_flag
            )
            if should_be_deleted_if_balance_is_negligible:
                if creditor_id == ROOT_CREDITOR_ID:  # pragma: nocover
                    should_be_deleted = row[c_principal] == 0
                else:
                    balance = calc_current_balance(
                        creditor_id=creditor_id,
                        principal=row[c_principal],
                        interest=row[c_interest],
                        interest_rate=row[c_interest_rate],
                        last_change_ts=row[c_last_change_ts],
                        current_ts=current_ts,
                    )
                    should_be_deleted = is_negligible_balance(
                        balance, row[c_negligible_amount]
                    )

            if should_be_deleted:
                chores.append(
                    create_chore_message(
                        {
                            "type": "TryToDeleteAccount",
                            "debtor_id": row[c_debtor_id],
                            "creditor_id": creditor_id,
                        }
                    )
                )

        chores_publisher.publish_messages(chores)

    def _capitalize_interests(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_last_interest_capitalization_ts = c.last_interest_capitalization_ts
        c_principal = c.principal
        c_interest = c.interest
        c_interest_rate = c.interest_rate
        c_last_change_ts = c.last_change_ts
        c_status_flags = c.status_flags
        deleted_flag = Account.STATUS_DELETED_FLAG
        cutoff_ts = current_ts - self.min_interest_cap_interval
        max_ratio = self.max_interest_to_principal_ratio
        chores = []

        for row in rows:
            creditor_id = row[c_creditor_id]
            can_capitalize_interest = (
                creditor_id != ROOT_CREDITOR_ID
                and row[c_last_interest_capitalization_ts] <= cutoff_ts
                and not row[c_status_flags] & deleted_flag
            )
            if can_capitalize_interest:
                current_balance = calc_current_balance(
                    creditor_id=creditor_id,
                    principal=row[c_principal],
                    interest=row[c_interest],
                    interest_rate=row[c_interest_rate],
                    last_change_ts=row[c_last_change_ts],
                    current_ts=current_ts,
                )
                accumulated_interest = abs(
                    contain_principal_overflow(
                        math.floor(current_balance - row[c_principal])
                    )
                )
                ratio = accumulated_interest / (1 + abs(row[c_principal]))

                if ratio > max_ratio:
                    chores.append(
                        create_chore_message(
                            {
                                "type": "CapitalizeInterest",
                                "debtor_id": row[c_debtor_id],
                                "creditor_id": creditor_id,
                            }
                        )
                    )

        chores_publisher.publish_messages(chores)

    def _change_debtor_settings(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_last_interest_rate_change_ts = c.last_interest_rate_change_ts
        c_status_flags = c.status_flags
        c_interest_rate = c.interest_rate
        c_debtor_info_iri = c.debtor_info_iri
        c_debtor_info_content_type = c.debtor_info_content_type
        c_debtor_info_sha256 = c.debtor_info_sha256
        deleted_flag = Account.STATUS_DELETED_FLAG
        interest_rate_change_cutoff_ts = (
            current_ts - self.interest_rate_change_min_interval
        )

        def should_change_interest_rate(row, current_interest_rate):
            return (
                row[c_interest_rate] != current_interest_rate
                and row[c_last_interest_rate_change_ts]
                <= interest_rate_change_cutoff_ts
                and not row[c_status_flags] & deleted_flag
            )

        def should_update_debtor_info(
            row, debtor_info_iri, debtor_info_content_type, debtor_info_sha256
        ):
            return not row[c_status_flags] & deleted_flag and (
                row[c_debtor_info_iri] != debtor_info_iri
                or row[c_debtor_info_content_type] != debtor_info_content_type
                or row[c_debtor_info_sha256] != debtor_info_sha256
            )

        debtor_ids = {row[c_debtor_id] for row in rows}
        config_data_dict = get_root_config_data_dict(debtor_ids)
        chores = []

        for row in rows:
            creditor_id = row[c_creditor_id]
            if creditor_id == ROOT_CREDITOR_ID:
                continue

            debtor_id = row[c_debtor_id]
            config_data = config_data_dict.get(debtor_id)
            if config_data:
                interest_rate = config_data.interest_rate_target
                if should_change_interest_rate(row, interest_rate):
                    chores.append(
                        create_chore_message(
                            {
                                "type": "ChangeInterestRate",
                                "debtor_id": debtor_id,
                                "creditor_id": creditor_id,
                                "interest_rate": interest_rate,
                                "ts": current_ts,
                            }
                        )
                    )

                debtor_info_iri = config_data.info_iri
                debtor_info_content_type = config_data.info_content_type
                debtor_info_sha256 = config_data.info_sha256
                if should_update_debtor_info(
                    row,
                    debtor_info_iri,
                    debtor_info_content_type,
                    debtor_info_sha256,
                ):
                    chores.append(
                        create_chore_message(
                            {
                                "type": "UpdateDebtorInfo",
                                "debtor_id": debtor_id,
                                "creditor_id": creditor_id,
                                "debtor_info_iri": debtor_info_iri or "",
                                "debtor_info_content_type": (
                                    debtor_info_content_type or ""
                                ),
                                "debtor_info_sha256": (
                                    b16encode(debtor_info_sha256).decode()
                                    if debtor_info_sha256
                                    else ""
                                ),
                                "ts": current_ts,
                            }
                        )
                    )

        chores_publisher.publish_messages(chores)


class PreparedTransferScanner(TableScanner):
    """Attempts to finalize staled prepared transfers."""

    table = PreparedTransfer.__table__
    pk = tuple_(
        PreparedTransfer.debtor_id,
        PreparedTransfer.sender_creditor_id,
        PreparedTransfer.transfer_id,
    )

    def __init__(self):
        super().__init__()

        # To prevent clogging the signal bus with remainder signals,
        # we ensure that the remainder interval is not shorter than
        # the allowed delay in the signal bus.
        self.remainder_interval = max(
            timedelta(
                days=current_app.config["APP_PREPARED_TRANSFER_REMAINDER_DAYS"]
            ),
            timedelta(days=current_app.config["APP_MESSAGE_MAX_DELAY_DAYS"]),
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_PREPARED_TRANSFERS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config["APP_PREPARED_TRANSFERS_SCAN_BEAT_MILLISECS"]

    def process_rows(self, rows):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_sender_creditor_id = c.sender_creditor_id
        c_last_reminder_ts = c.last_reminder_ts
        c_prepared_at = c.prepared_at
        current_ts = datetime.now(tz=timezone.utc)
        reminder_cutoff_ts = current_ts - self.remainder_interval
        prepared_transfer_signal_mappings = {}

        for row in rows:
            if not is_valid_account(
                    row[c_debtor_id], row[c_sender_creditor_id]
            ):
                continue  # pragma: no cover

            last_reminder_ts = row[c_last_reminder_ts]
            has_big_delay = row[c_prepared_at] < reminder_cutoff_ts
            has_recent_reminder = (
                last_reminder_ts is not None
                and last_reminder_ts >= reminder_cutoff_ts
            )

            if has_big_delay and not has_recent_reminder:
                debtor_id = row[c.debtor_id]
                sender_creditor_id = row[c.sender_creditor_id]
                transfer_id = row[c.transfer_id]

                prepared_transfer_signal_mappings[
                    (debtor_id, sender_creditor_id, transfer_id)
                ] = dict(
                    debtor_id=debtor_id,
                    sender_creditor_id=sender_creditor_id,
                    transfer_id=transfer_id,
                    coordinator_type=row[c.coordinator_type],
                    coordinator_id=row[c.coordinator_id],
                    coordinator_request_id=row[c.coordinator_request_id],
                    locked_amount=row[c.locked_amount],
                    recipient_creditor_id=row[c.recipient_creditor_id],
                    prepared_at=row[c_prepared_at],
                    demurrage_rate=row[c.demurrage_rate],
                    deadline=row[c.deadline],
                    final_interest_rate_ts=row[c.final_interest_rate_ts],
                    inserted_at=max(current_ts, row[c_prepared_at]),
                )

        if prepared_transfer_signal_mappings:
            chosen = PreparedTransfer.choose_rows(
                list(prepared_transfer_signal_mappings.keys())
            )
            pks_to_update = {
                (pt.debtor_id, pt.sender_creditor_id, pt.transfer_id)
                for pt in db.session.execute(
                        select(
                            PreparedTransfer.debtor_id,
                            PreparedTransfer.sender_creditor_id,
                            PreparedTransfer.transfer_id,
                        )
                        .select_from(PreparedTransfer)
                        .join(chosen, self.pk == tuple_(*chosen.c))
                        .with_for_update(skip_locked=True, key_share=True)
                ).all()
            }
            if pks_to_update:
                to_update = PreparedTransfer.choose_rows(list(pks_to_update))
                db.session.execute(
                    update(PreparedTransfer)
                    .execution_options(synchronize_session=False)
                    .where(self.pk == tuple_(*to_update.c))
                    .values(last_reminder_ts=current_ts)
                )
                db.session.execute(
                    insert(PreparedTransferSignal)
                    .execution_options(
                        insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                        synchronize_session=False,
                    ),
                    [
                        v
                        for k, v in prepared_transfer_signal_mappings.items()
                        if k in pks_to_update
                    ],
                )

            db.session.commit()
            db.session.close()


class RegisteredBalanceChangeScanner(TableScanner):
    """Attempts to delete stale registered balance changes."""

    table = RegisteredBalanceChange.__table__
    pk = tuple_(
        RegisteredBalanceChange.debtor_id,
        RegisteredBalanceChange.other_creditor_id,
        RegisteredBalanceChange.change_id,
    )

    def __init__(self):
        super().__init__()
        self.cutoff_ts = current_app.config[
            "REMOVE_FROM_ARCHIVE_THRESHOLD_DATE"
        ]

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_REGISTERED_BALANCE_CHANGES_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_REGISTERED_BALANCE_CHANGES_SCAN_BEAT_MILLISECS"
        ]

    def process_rows(self, rows):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_other_creditor_id = c.other_creditor_id
        c_change_id = c.change_id
        c_committed_at = c.committed_at
        c_is_applied = c.is_applied
        cutoff_ts = self.cutoff_ts

        pks_to_delete = [
            (row[c_debtor_id], row[c_other_creditor_id], row[c_change_id])
            for row in rows
            if (row[c_committed_at] < cutoff_ts and row[c_is_applied])
        ]
        if pks_to_delete:
            chosen = RegisteredBalanceChange.choose_rows(pks_to_delete)
            db.session.execute(
                delete(RegisteredBalanceChange)
                .execution_options(synchronize_session=False)
                .where(self.pk == tuple_(*chosen.c))
            )
            db.session.commit()
            db.session.close()
