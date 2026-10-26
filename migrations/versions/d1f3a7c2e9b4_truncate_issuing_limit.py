"""truncate issuing limit

Revision ID: d1f3a7c2e9b4
Revises: a5202da9c3ad
Create Date: 2026-10-26 19:30:00.000000

"""
from alembic import op
import sqlalchemy as sa

from swpt_accounts.migration_helpers import ReplaceableObject

# revision identifiers, used by Alembic.
revision = 'd1f3a7c2e9b4'
down_revision = 'a5202da9c3ad'
branch_labels = None
depends_on = None


# NOTE: Casting a fractional number (like "100.5") directly to
# `NUMERIC(24)` rounds it (to 101). The Python implementation
# (`parse_root_config_data`) truncates it (to 100). Here we truncate
# the value too, so that both implementations agree.
get_min_account_balance_sp = ReplaceableObject(
    "get_min_account_balance(acc account_data)",
    """
    RETURNS BIGINT AS $$
    DECLARE
      issuing_limit NUMERIC(24);
    BEGIN
      IF acc.creditor_id=0 THEN
        BEGIN
          issuing_limit := trunc(CAST(
            acc.config_data::JSON ->> 'limit' AS NUMERIC
          ));
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


def upgrade():
    op.replace_sp(get_min_account_balance_sp, replaces="7a49b06e1eb6.get_min_account_balance_sp")


def downgrade():
    op.replace_sp(get_min_account_balance_sp, replace_with="7a49b06e1eb6.get_min_account_balance_sp")
