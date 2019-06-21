import click
from flask.cli import with_appcontext
from . import procedures


@click.group()
def swpt_accounts():
    """Perform operations on Swaptacular accounts."""


@swpt_accounts.command()
@with_appcontext
@click.option('-p', '--processes', type=int, help='The number of worker processes.')
def process_pending_changes(processes):
    """Process all pending account changes."""

    changes = procedures.get_accounts_with_pending_changes()

    # TODO: Spawn worker processes.
    for debtor_id, creditor_id in changes:
        procedures.process_pending_changes(debtor_id, creditor_id)
