"""Helpers for managing stored procedures and views in migration files.

See alembic's documentation:
https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects
"""

from dataclasses import dataclass
from alembic.operations import Operations, MigrateOperation


@dataclass
class ReplaceableObject:
    """Represents a database stored procedure or a database view."""

    name: str
    sqltext: str


class ReversibleOp(MigrateOperation):  # pragma: no cover
    def __init__(self, target):
        self.target = target

    @classmethod
    def invoke_for_target(cls, operations, target):
        op = cls(target)
        return operations.invoke(op)

    def reverse(self):
        raise NotImplementedError()

    @classmethod
    def _get_object_from_version(cls, operations, ident):
        version, objname = ident.split(".")

        module = operations.get_context().script.get_revision(version).module
        obj = getattr(module, objname)
        return obj

    @classmethod
    def replace(cls, operations, target, replaces=None, replace_with=None):

        if replaces:
            old_obj = cls._get_object_from_version(operations, replaces)
            drop_old = cls(old_obj).reverse()
            create_new = cls(target)
        elif replace_with:
            old_obj = cls._get_object_from_version(operations, replace_with)
            drop_old = cls(target).reverse()
            create_new = cls(old_obj)
        else:
            raise TypeError("replaces or replace_with is required")

        operations.invoke(drop_old)
        operations.invoke(create_new)


@Operations.register_operation("create_view", "invoke_for_target")
@Operations.register_operation("replace_view", "replace")
class CreateViewOp(ReversibleOp):  # pragma: no cover
    def reverse(self):
        return DropViewOp(self.target)


@Operations.register_operation("drop_view", "invoke_for_target")
class DropViewOp(ReversibleOp):  # pragma: no cover
    def reverse(self):
        return CreateViewOp(self.target)


@Operations.register_operation("create_sp", "invoke_for_target")
@Operations.register_operation("replace_sp", "replace")
class CreateSPOp(ReversibleOp):  # pragma: no cover
    def reverse(self):
        return DropSPOp(self.target)


@Operations.register_operation("drop_sp", "invoke_for_target")
class DropSPOp(ReversibleOp):  # pragma: no cover
    def reverse(self):
        return CreateSPOp(self.target)


@Operations.register_operation("create_type", "invoke_for_target")
@Operations.register_operation("replace_type", "replace")
class CreateTypeOp(ReversibleOp):  # pragma: no cover
    def reverse(self):
        return DropTypeOp(self.target)


@Operations.register_operation("drop_type", "invoke_for_target")
class DropTypeOp(ReversibleOp):  # pragma: no cover
    def reverse(self):
        return CreateTypeOp(self.target)


@Operations.implementation_for(CreateViewOp)
def create_view(operations, operation):  # pragma: no cover
    operations.execute("CREATE VIEW %s AS %s" % (
        operation.target.name,
        operation.target.sqltext
    ))


@Operations.implementation_for(DropViewOp)
def drop_view(operations, operation):  # pragma: no cover
    operations.execute("DROP VIEW %s" % operation.target.name)


@Operations.implementation_for(CreateSPOp)
def create_sp(operations, operation):  # pragma: no cover
    operations.execute(
        "CREATE FUNCTION %s %s" % (
            operation.target.name, operation.target.sqltext
        )
    )


@Operations.implementation_for(DropSPOp)
def drop_sp(operations, operation):  # pragma: no cover
    operations.execute("DROP FUNCTION %s" % operation.target.name)


@Operations.implementation_for(CreateTypeOp)
def create_type(operations, operation):  # pragma: no cover
    operations.execute(
        "CREATE TYPE %s %s" % (
            operation.target.name, operation.target.sqltext
        )
    )


@Operations.implementation_for(DropTypeOp)
def drop_type(operations, operation):  # pragma: no cover
    operations.execute("DROP TYPE %s" % operation.target.name)
