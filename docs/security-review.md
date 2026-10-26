# Security and bug review: swpt_accounts

| | |
|---|---|
| Reviewed commit | `d4fba0b` (2026-04-01) |
| Review date | 2026-10-26 |
| Scope | `swpt_accounts/` (procedures, models, actors, chores, schemas, fetch API), PL/pgSQL procedures in `migrations/versions/7a49b06e1eb6_replace_procedures.py` |
| Method | Manual code review; the Python and PL/pgSQL implementations compared line by line; findings reproduced against PostgreSQL 16, with the test suite run in both modes (`--use-pgplsql=false` and `true`) |

This document covers the issues **fixed in this pull request**, plus
review notes.

## Summary

| ID | Severity | Finding | Status |
|---|---|---|---|
| SEC-1 | Low | One malformed `PrepareTransfer` message stops message processing for the whole shard | Fixed |
| BUG-1 | Low | PL/pgSQL rounds a fractional issuing limit up; Python truncates it | Fixed |

## SEC-1: Poison `PrepareTransfer` message halts the consumer (Low, fixed)

**Where:** `swpt_accounts/actors.py`, `_on_prepare_transfer_signal`.

**Problem.** For an `agent` transfer, the actor checks that the sender
and the coordinator are managed by the same agent, and **raises
`RuntimeError`** when they are not. `PrepareTransferMessageSchema`
(from `swpt_pythonlib`) validates `coordinator_id` for `direct` and
`issuing` transfers, but not for `agent` transfers, so such a message
passes schema validation and reaches the actor.

In `swpt_pythonlib.rabbitmq.Consumer`, an exception from
`process_message` stops the worker thread and terminates consumption.
The message is never acknowledged, so RabbitMQ redelivers it after the
restart, and the consumer stops again. A single message therefore
blocks the processing of every other message in the queue: transfers,
configurations and balance changes for all accounts of the shard.

**Reachability.** `swpt_stomp` checks that an `agent` transfer's
`coordinator_id` belongs to the sending peer's creditors subnet. With
the standard 24-bit creditors-agent subnets, that check blocks such
messages from remote peers. It remains reachable for a peer configured
with a wider subnet, or through any local component that publishes to
the accounts queue, so this is a defence-in-depth fix.

**Reproduction.** A schema-valid message with
`"coordinator_type": "agent"`, `"creditor_id": 1099511627778`
(subnet `0x0000010000…`) and `"coordinator_id": 2199023255553`
(subnet `0x0000020000…`):

```
SmpConsumer.process_message(body, props)
→ RuntimeError: Invalid coordinator ID for agent transfer.
```

**Fix.** The actor now logs an error and ignores the message, the same
way other invalid messages are handled. The message is acknowledged, so
processing continues. Adding the check to
`PrepareTransferMessageSchema` in `swpt_pythonlib` would also be
reasonable.

**Test:** `test_prepare_agent_transfer_invalid_coordinator`.

## BUG-1: Fractional issuing limit is rounded up by PL/pgSQL (Low, fixed)

**Where:** `get_min_account_balance` (PL/pgSQL, used in production
because `APP_USE_PGPLSQL_FUNCTIONS = True` by default), and
`parse_root_config_data` (Python).

**Problem.** The root account's `config_data` may contain
`"limit": 100.5`. Python validates it with a non-strict
`fields.Integer`, which truncates to **100**. The stored procedure
casts the text straight to `NUMERIC(24)`, which rounds to **101**. With
the default PL/pgSQL mode, the issuer could therefore issue one more
unit than the Python implementation, and the test suite in its default
mode, allow.

```
parse_root_config_data('{"limit": 100.5}').issuing_limit   → 100
CAST('{"limit": 100.5}'::json ->> 'limit' AS NUMERIC(24))  → 101
```

The issuing limit is only one of two bounds. The other bound,
`negligible_amount`, which the debtors agent sets from the
administrator's restriction, is not affected.

**Fix.** A new migration (`d1f3a7c2e9b4`) replaces
`get_min_account_balance` so that it truncates the value. Downgrade
restores the previous procedure.

**Test:** `test_root_config_fractional_issuing_limit` fails on the old
procedure with `--use-pgplsql=true`, and passes in both modes with the
migration.

## Review notes

- **Python and PL/pgSQL equivalence.** Apart from BUG-1, the transfer
  preparation, finalization, pending-balance-change and
  interest-accrual logic is equivalent in both implementations,
  including overflow containment, the agent subnet check, demurrage and
  the `expendable_amount` calculation.
- **Negligible-transfer threshold (informational).** PL/pgSQL compares
  `acquired_amount::REAL <= negligible_amount`. For amounts above
  2<sup>24</sup>, the `REAL` cast can make a transfer just above the
  threshold count as negligible, which skips its `AccountTransfer`
  notification. Python compares in double precision. This is not fixed
  here.
- **Recipient parsing (informational).** `int(recipient)` also accepts
  forms such as `" 5"`, `"+5"` and `"5_0"`, so one account can be named
  in several ways. This did not lead to a misdirected transfer.
- **Chores consumer.** Chores for accounts outside the shard are
  ignored, which correctly prevents double interest payments after a
  shard split.

## Tests

- `test_prepare_agent_transfer_invalid_coordinator`
- `test_root_config_fractional_issuing_limit` (2 cases)

Full suite: 106 passed, 2 skipped, in both Python and PL/pgSQL modes;
`flake8` clean. The fetch-API tests need a running fetch server
(`FETCH_API_URL`).
