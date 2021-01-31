import math
from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_lib.scan_table import TableScanner
from sqlalchemy.sql.expression import true, tuple_, or_
from flask import current_app
from swpt_accounts.extensions import db
from swpt_accounts.models import Account, AccountUpdateSignal, AccountPurgeSignal, PreparedTransfer, \
    PreparedTransferSignal, ROOT_CREDITOR_ID, calc_current_balance, is_negligible_balance, \
    contain_principal_overflow
from swpt_accounts.fetch_api_client import get_root_config_data_dict
from swpt_accounts.chores import change_interest_rate, capitalize_interest, try_to_delete_account

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


class AccountScanner(TableScanner):
    """Sends account heartbeat signals, purge deleted accounts."""

    table = Account.__table__
    pk = tuple_(Account.debtor_id, Account.creditor_id)

    def __init__(self):
        super().__init__()
        signalbus_max_delay = timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'])
        account_heartbeat_interval = timedelta(days=current_app.config['APP_ACCOUNT_HEARTBEAT_DAYS'])

        self.account_purge_delay = (
            timedelta(days=current_app.config['APP_INTRANET_EXTREME_DELAY_DAYS'])
            + timedelta(days=current_app.config['APP_PREPARED_TRANSFER_MAX_DELAY_DAYS'])
            + timedelta(days=2)
        )
        self.few_days_interval = timedelta(days=3)
        self.deletion_attempts_min_interval = timedelta(days=current_app.config['APP_DELETION_ATTEMPTS_MIN_DAYS'])
        self.interest_rate_change_min_interval = signalbus_max_delay + timedelta(days=1)
        self.max_interest_to_principal_ratio = current_app.config['APP_MAX_INTEREST_TO_PRINCIPAL_RATIO']
        self.min_interest_cap_interval = timedelta(days=current_app.config['APP_MIN_INTEREST_CAPITALIZATION_DAYS'])

        # To prevent clogging the signal bus with heartbeat signals,
        # we ensure that the account heartbeat interval is not shorter
        # than the allowed delay in the signal bus.
        self.account_heartbeat_interval = max(account_heartbeat_interval, signalbus_max_delay)

        assert self.max_interest_to_principal_ratio > 0.0

    @property
    def blocks_per_query(self) -> int:
        return current_app.config['APP_ACCOUNTS_SCAN_BLOCKS_PER_QUERY']

    @property
    def target_beat_duration(self) -> int:
        return current_app.config['APP_ACCOUNTS_SCAN_BEAT_MILLISECS']

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        self._purge_accounts(rows, current_ts)
        self._send_heartbeats(rows, current_ts)
        self._delete_accounts(rows, current_ts)
        self._capitalize_interests(rows, current_ts)
        self._change_interest_rates(rows, current_ts)

    def _purge_accounts(self, rows, current_ts):
        c = self.table.c
        deleted_flag = Account.STATUS_DELETED_FLAG
        date_few_days_ago = (current_ts - self.few_days_interval).date()
        purge_cutoff_ts = current_ts - self.account_purge_delay

        # If an account is created, deleted, purged, and re-created in
        # a single day, the `creation_date` of the new account will be
        # the same as the `creation_date` of the old account. We need
        # to make sure this never happens.
        pks_to_purge = [(row[c.debtor_id], row[c.creditor_id]) for row in rows if (
            row[c.status_flags] & deleted_flag
            and row[c.last_change_ts] < purge_cutoff_ts
            and row[c.creation_date] < date_few_days_ago)
        ]

        if pks_to_purge:
            to_purge = db.session.query(Account.debtor_id, Account.creditor_id, Account.creation_date).\
                filter(self.pk.in_(pks_to_purge)).\
                filter(Account.status_flags.op('&')(deleted_flag) == deleted_flag).\
                filter(Account.last_change_ts < purge_cutoff_ts).\
                filter(Account.creation_date < date_few_days_ago).\
                with_for_update().\
                all()

            if to_purge:
                pks_to_purge = [(debtor_id, creditor_id) for debtor_id, creditor_id, _ in to_purge]
                Account.query.\
                    filter(self.pk.in_(pks_to_purge)).\
                    delete(synchronize_session=False)

                db.session.bulk_insert_mappings(AccountPurgeSignal, [
                    dict(
                        debtor_id=debtor_id,
                        creditor_id=creditor_id,
                        creation_date=creation_date,
                    )
                    for debtor_id, creditor_id, creation_date in to_purge
                ])

    def _send_heartbeats(self, rows, current_ts):
        c = self.table.c
        deleted_flag = Account.STATUS_DELETED_FLAG
        heartbeat_cutoff_ts = current_ts - self.account_heartbeat_interval

        pks_to_heartbeat = [(row[c.debtor_id], row[c.creditor_id]) for row in rows if (
            not row[c.status_flags] & deleted_flag
            and (row[c.last_heartbeat_ts] < heartbeat_cutoff_ts or row[c.pending_account_update])
        )]

        if pks_to_heartbeat:
            to_heartbeat = Account.query.\
                filter(self.pk.in_(pks_to_heartbeat)).\
                filter(Account.status_flags.op('&')(deleted_flag) == 0).\
                filter(or_(
                    Account.last_heartbeat_ts < heartbeat_cutoff_ts,
                    Account.pending_account_update == true(),
                )).\
                with_for_update().\
                all()

            if to_heartbeat:
                pks_to_remind = [(account.debtor_id, account.creditor_id) for account in to_heartbeat]
                Account.query.\
                    filter(self.pk.in_(pks_to_remind)).\
                    update({
                        Account.last_heartbeat_ts: current_ts,
                        Account.pending_account_update: False,
                    }, synchronize_session=False)

                db.session.bulk_insert_mappings(AccountUpdateSignal, [
                    dict(
                        debtor_id=account.debtor_id,
                        creditor_id=account.creditor_id,
                        last_change_seqnum=account.last_change_seqnum,
                        last_change_ts=account.last_change_ts,
                        principal=account.principal,
                        interest=account.interest,
                        interest_rate=account.interest_rate,
                        last_interest_rate_change_ts=account.last_interest_rate_change_ts,
                        last_transfer_number=account.last_transfer_number,
                        last_transfer_committed_at=account.last_transfer_committed_at,
                        last_config_ts=account.last_config_ts,
                        last_config_seqnum=account.last_config_seqnum,
                        creation_date=account.creation_date,
                        negligible_amount=account.negligible_amount,
                        config_data=account.config_data,
                        config_flags=account.config_flags,
                        debtor_info_iri=account.debtor_info_iri,
                        debtor_info_content_type=account.debtor_info_content_type,
                        debtor_info_sha256=account.debtor_info_sha256,
                        inserted_at=max(current_ts, account.last_change_ts),
                    )
                    for account in to_heartbeat
                ])

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
        scheduled_for_deletion_flag = Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        deleted_flag = Account.STATUS_DELETED_FLAG
        cutoff_ts = current_ts - self.deletion_attempts_min_interval

        for row in rows:
            creditor_id = row[c_creditor_id]
            should_be_deleted = (
                creditor_id != ROOT_CREDITOR_ID
                and row[c_last_deletion_attempt_ts] <= cutoff_ts
                and row[c_config_flags] & scheduled_for_deletion_flag
                and not row[c_status_flags] & deleted_flag
                and is_negligible_balance(
                    calc_current_balance(
                        creditor_id=creditor_id,
                        principal=row[c_principal],
                        interest=row[c_interest],
                        interest_rate=row[c_interest_rate],
                        last_change_ts=row[c_last_change_ts],
                        current_ts=current_ts,
                    ),
                    row[c_negligible_amount],
                )
            )
            if should_be_deleted:
                try_to_delete_account.send(row[c_debtor_id], creditor_id)

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
                accumulated_interest = abs(contain_principal_overflow(math.floor(current_balance - row[c_principal])))
                ratio = accumulated_interest / (1 + abs(row[c_principal]))

                if ratio > max_ratio:
                    capitalize_interest.send(row[c_debtor_id], creditor_id)

    def _change_interest_rates(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_last_interest_rate_change_ts = c.last_interest_rate_change_ts
        c_status_flags = c.status_flags
        c_interest_rate = c.interest_rate
        deleted_flag = Account.STATUS_DELETED_FLAG
        cutoff_ts = current_ts - self.interest_rate_change_min_interval

        def can_change_interest_rate(row):
            return (
                row[c_creditor_id] != ROOT_CREDITOR_ID
                and row[c_last_interest_rate_change_ts] <= cutoff_ts
                and not row[c_status_flags] & deleted_flag
            )

        debtor_ids = {row[c_debtor_id] for row in rows if can_change_interest_rate(row)}
        config_data_dict = get_root_config_data_dict(debtor_ids)

        for row in rows:
            debtor_id = row[c_debtor_id]
            config_data = config_data_dict.get(debtor_id)

            if config_data and can_change_interest_rate(row):
                interest_rate = config_data.interest_rate

                if row[c_interest_rate] != interest_rate:
                    change_interest_rate.send(debtor_id, row[c_creditor_id], interest_rate, current_ts.isoformat())


class PreparedTransferScanner(TableScanner):
    """Attempts to finalize staled prepared transfers."""

    table = PreparedTransfer.__table__
    pk = tuple_(PreparedTransfer.debtor_id, PreparedTransfer.sender_creditor_id, PreparedTransfer.transfer_id)

    def __init__(self):
        super().__init__()

        # To prevent clogging the signal bus with remainder signals,
        # we ensure that the remainder interval is not shorter than
        # the allowed delay in the signal bus.
        self.remainder_interval = max(
            timedelta(days=current_app.config['APP_PREPARED_TRANSFER_REMAINDER_DAYS']),
            timedelta(days=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS']),
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config['APP_PREPARED_TRANSFERS_SCAN_BLOCKS_PER_QUERY']

    @property
    def target_beat_duration(self) -> int:
        return current_app.config['APP_PREPARED_TRANSFERS_SCAN_BEAT_MILLISECS']

    @atomic
    def process_rows(self, rows):
        c = self.table.c
        c_last_reminder_ts = c.last_reminder_ts
        c_prepared_at = c.prepared_at
        current_ts = datetime.now(tz=timezone.utc)
        reminder_cutoff_ts = current_ts - self.remainder_interval
        prepared_transfer_signal_mappings = {}

        for row in rows:
            last_reminder_ts = row[c_last_reminder_ts]
            has_big_delay = row[c_prepared_at] < reminder_cutoff_ts
            has_recent_reminder = last_reminder_ts is not None and last_reminder_ts >= reminder_cutoff_ts

            if has_big_delay and not has_recent_reminder:
                debtor_id = row[c.debtor_id]
                sender_creditor_id = row[c.sender_creditor_id]
                transfer_id = row[c.transfer_id]

                prepared_transfer_signal_mappings[(debtor_id, sender_creditor_id, transfer_id)] = dict(
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
                    min_interest_rate=row[c.min_interest_rate],
                    inserted_at=max(current_ts, row[c_prepared_at]),
                )

        if prepared_transfer_signal_mappings:
            pks_to_remind = prepared_transfer_signal_mappings.keys()
            PreparedTransfer.query.\
                filter(self.pk.in_(pks_to_remind)).\
                update({
                    PreparedTransfer.last_reminder_ts: current_ts,
                }, synchronize_session=False)

            db.session.bulk_insert_mappings(PreparedTransferSignal, prepared_transfer_signal_mappings.values())
