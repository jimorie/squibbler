from __future__ import annotations

import functools
import typing


if typing.TYPE_CHECKING:
    from typing import Any, Callable, Protocol

    RawType = str | int | float | bool | None
    AnyTerm = 'Term' | RawType
    AnyTerms = list[AnyTerm]


def wrap_operand(term: AnyTerm) -> Term:
    """Wrap operator arguments as `Parameter` or grouped objects, as needed."""
    if isinstance(term, (Query, CompositeTerm)):
        return term.group()
    if isinstance(term, Term):
        return term
    return Parameter(term)


def operator(func: Callable) -> Callable:
    """Decorator that calls `wrap_operand` for each argument."""

    @functools.wraps(func)
    def decorator(*args: list[OperatorTerm]) -> OperatorTerm:
        return func(*(wrap_operand(arg) for arg in args))

    return decorator


class Context(dict):
    """
    Class used to resolve parameters and literals while compiling terms to
    SQL. It acts as a `dict` with all resolved parameter values stored in
    it.

    This class can be extended to provide dialectal differences in how
    parameters, literals and operators are resolved.
    """

    STRING_DELIMITER = "'"
    DIALECT_OPERATORS = {}
    QUERY_MODE = None

    def resolve_param(self, value: RawType) -> str:
        """
        Return a new parameter name for `value` and store it in `self` for use
        in parameterized queries.
        """
        name = str(len(self) + 1)
        self[name] = value
        return f":{name}"

    @classmethod
    def resolve_literal(cls, value: RawType) -> str:
        """Return `value` as a SQL literal."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return str(int(value))
        if isinstance(value, str):
            return f"'{value.replace(cls.STRING_DELIMITER, cls.STRING_DELIMITER * 2)}'"
        return str(value)


class Term:
    """Base class for all terms that make up the changeable parts of a SQL query."""

    def sql(self, ctx: Context) -> str:
        """
        Abstract method for converting this term to SQL with the given `ctx`.
        Must be provided by implementing classes.

        Examples:
        >>> Term().sql(Context())
        Traceback (most recent call last):
            ...
        NotImplementedError
        """
        raise NotImplementedError


class OperatorTerm(Term):
    """
    Base class for all terms that can be used together with operators to
    form other terms.
    """

    def group(self) -> CompositeTerm:
        """Apply parantheses around this term.

        Examples:
        >>> Literal(42).group().sql(Context())
        '(42)'
        """
        return CompositeTerm("({})", self)

    def alias(self, alias: str) -> CompositeTerm:
        """Apply a name to this term.

        Examples:
        >>> Column('foo').alias('bar').sql(Context())
        'foo AS bar'
        """
        return CompositeTerm("{} AS {}", self, RawSql(alias))

    def asc(self) -> CompositeTerm:
        """Apply the ASC postfix to this term.

        Examples:
        >>> Column('foo').asc().sql(Context())
        'foo ASC'
        """
        return CompositeTerm("{} ASC", self)

    def desc(self) -> CompositeTerm:
        """Apply the DESC postfix to this term.

        Examples:
        >>> Column('foo').desc().sql(Context())
        'foo DESC'
        """
        return CompositeTerm("{} DESC", self)

    def max(self) -> CompositeTerm:
        """Apply the MAX aggregate function to this term.

        Examples:
        >>> Column('foo').max().sql(Context())
        'MAX(foo)'
        """
        return CompositeTerm("MAX({})", self)

    def min(self) -> CompositeTerm:
        """Apply the MIN aggregate function to this term.

        Examples:
        >>> Column('foo').min().sql(Context())
        'MIN(foo)'
        """
        return CompositeTerm("MIN({})", self)

    def count(self) -> CompositeTerm:
        """Apply the COUNT aggregate function to this term.

        Examples:
        >>> Column('foo').count().sql(Context())
        'COUNT(foo)'
        """
        return CompositeTerm("COUNT({})", self)

    def sum(self) -> CompositeTerm:
        """Apply the SUM aggregate function to this term.

        Examples:
        >>> Column('foo').sum().sql(Context())
        'SUM(foo)'
        """
        return CompositeTerm("SUM({})", self)

    def isnull(self) -> CompositeTerm:
        """Apply an IS NULL test to this term.

        Examples:
        >>> Column('foo').isnull().sql(Context())
        'foo IS NULL'
        """
        return CompositeTerm("{} IS NULL", self)

    def isnotnull(self) -> CompositeTerm:
        """Apply an IS NOT NULL test to this term.

        Examples:
        >>> Column('foo').isnotnull().sql(Context())
        'foo IS NOT NULL'
        """
        return CompositeTerm("{} IS NOT NULL", self)

    def in_(self, other: OperatorTerm | list, not_: bool = False) -> CompositeTerm:
        """Apply an IN test between this term and `other`.

        Examples:
        >>> Column('foo').in_([1, 2, 3]).sql(Context())
        'foo IN (:1, :2, :3)'
        """
        if isinstance(other, (tuple, list)):
            other = JoinedTerm([wrap_operand(t) for t in other]).group()
        else:
            other = wrap_operand(other)
        formatstr = "{} NOT IN {}" if not_ else "{} IN {}"
        return CompositeTerm(formatstr, self, other)

    def notin(self, other: OperatorTerm | list) -> CompositeTerm:
        """Apply an NOT IN test between this term and `other`.

        Examples:
        >>> Column('foo').notin([1, 2, 3]).sql(Context())
        'foo NOT IN (:1, :2, :3)'
        """
        return self.in_(other, True)

    def contains(self, substr: str) -> CompositeTerm:
        """Apply a contains test between this term and `substr`."""
        return self.like(f"%{substr}%")

    def startswith(self, prefix: str) -> CompositeTerm:
        """Apply a startswith test between this term and `prefix`."""
        return self.like(f"{prefix}%")

    def endswith(self, postfix: str) -> CompositeTerm:
        """Apply a endswith test between this term and `postfix`."""
        return self.like(f"%{postfix}")

    def like(self, other: str) -> CompositeTerm:
        """Apply a LIKE test between this term and `other`.

        Examples:
        >>> Column('foo').like('b_r').sql(Context())
        'foo LIKE :1'
        """
        return CompositeTerm("{} LIKE {}", self, wrap_operand(other))

    @operator
    def and_(self: OperatorTerm, other: OperatorTerm) -> CompositeTerm:
        """Apply an AND test between this term and `other`.

        Examples:
        >>> Column('foo').and_(Column('bar')).sql(Context())
        'foo AND bar'
        >>> (Column('foo') & Column('bar')).sql(Context())
        'foo AND bar'
        """
        return CompositeTerm("{} AND {}", self, other)

    @operator
    def or_(self, other: OperatorTerm) -> CompositeTerm:
        """Apply an OR test between this term and `other`.

        Examples:
        >>> Column('foo').or_(Column('bar')).sql(Context())
        'foo OR bar'
        >>> (Column('foo') | Column('bar')).sql(Context())
        'foo OR bar'
        """
        return CompositeTerm("{} OR {}", self, other)

    @operator
    def lt(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a less-than test between this term and `other`.

        Examples:
        >>> Literal(42).lt(Literal(43)).sql(Context())
        '42 < 43'
        >>> Literal(42).lt(43).sql(Context())
        '42 < :1'
        >>> (Literal(42) < 43).sql(Context())
        '42 < :1'
        >>> (42 < Literal(43)).sql(Context())
        '43 > :1'
        """
        return CompositeTerm("{} < {}", self, other)

    @operator
    def le(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a less-than-or-equal test between this term and `other`.

        Examples:
        >>> Literal(42).le(Literal(43)).sql(Context())
        '42 <= 43'
        >>> Literal(42).le(43).sql(Context())
        '42 <= :1'
        >>> (Literal(42) <= 43).sql(Context())
        '42 <= :1'
        >>> (42 <= Literal(43)).sql(Context())
        '43 >= :1'
        """
        return CompositeTerm("{} <= {}", self, other)

    @operator
    def eq(self, other: OperatorTerm) -> CompositeTerm:
        """Apply an equality test between this term and `other`.

        Examples:
        >>> Literal(42).eq(Literal(43)).sql(Context())
        '42 = 43'
        >>> Literal(42).eq(43).sql(Context())
        '42 = :1'
        >>> (Literal(42) == 43).sql(Context())
        '42 = :1'
        >>> (42 == Literal(43)).sql(Context())
        '43 = :1'
        """
        return CompositeTerm("{} = {}", self, other)

    @operator
    def ne(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a non-equality test between this term and `other`.

        Examples:
        >>> Literal(42).ne(Literal(43)).sql(Context())
        '42 <> 43'
        >>> Literal(42).ne(43).sql(Context())
        '42 <> :1'
        >>> (Literal(42) != 43).sql(Context())
        '42 <> :1'
        >>> (42 != Literal(43)).sql(Context())
        '43 <> :1'
        """
        return CompositeTerm("{} <> {}", self, other)

    @operator
    def gt(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a greater-than test between this term and `other`.

        Examples:
        >>> Literal(42).gt(Literal(43)).sql(Context())
        '42 > 43'
        >>> Literal(42).gt(43).sql(Context())
        '42 > :1'
        >>> (Literal(42) > 43).sql(Context())
        '42 > :1'
        >>> (42 > Literal(43)).sql(Context())
        '43 < :1'
        """
        return CompositeTerm("{} > {}", self, other)

    @operator
    def ge(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a greater-than-or-equal test between this term and `other`.

        Examples:
        >>> Literal(42).ge(Literal(43)).sql(Context())
        '42 >= 43'
        >>> Literal(42).ge(43).sql(Context())
        '42 >= :1'
        >>> (Literal(42) >= 43).sql(Context())
        '42 >= :1'
        >>> (42 >= Literal(43)).sql(Context())
        '43 <= :1'
        """
        return CompositeTerm("{} >= {}", self, other)

    @operator
    def add(self, other: OperatorTerm) -> CompositeTerm:
        """Apply an addition operation between this term and `other`.

        Examples:
        >>> Literal(42).add(Literal(43)).sql(Context())
        '42 + 43'
        >>> Literal(42).add(43).sql(Context())
        '42 + :1'
        >>> (Literal(42) + 43).sql(Context())
        '42 + :1'
        >>> (42 + Literal(43)).sql(Context())
        ':1 + 43'
        """
        return CompositeTerm("{} + {}", self, other)

    @operator
    def sub(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a subtraction operation between this term and `other`.

        Examples:
        >>> Literal(42).sub(Literal(43)).sql(Context())
        '42 - 43'
        >>> (Literal(42) - 43).sql(Context())
        '42 - :1'
        >>> (42 - Literal(43)).sql(Context())
        ':1 - 43'
        """
        return CompositeTerm("{} - {}", self, other)

    @operator
    def mul(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a multiplication operation between this term and `other`.

        Examples:
        >>> Literal(42).mul(Literal(43)).sql(Context())
        '42 * 43'
        >>> (Literal(42) * 43).sql(Context())
        '42 * :1'
        >>> (42 * Literal(43)).sql(Context())
        ':1 * 43'
        """
        return CompositeTerm("{} * {}", self, other)

    @operator
    def div(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a division operation between this term and `other`.

        Examples:
        >>> Literal(42).div(Literal(43)).sql(Context())
        '42 / 43'
        >>> (Literal(42) / 43).sql(Context())
        '42 / :1'
        >>> (42 / Literal(43)).sql(Context())
        ':1 / 43'
        """
        return CompositeTerm("{} / {}", self, other)

    @operator
    def mod(self, other: OperatorTerm) -> CompositeTerm:
        """Apply a modulo operation between this term and `other`.

        Examples:
        >>> Literal(42).mod(Literal(43)).sql(Context())
        '42 % 43'
        >>> (Literal(42) % 43).sql(Context())
        '42 % :1'
        >>> (42 % Literal(43)).sql(Context())
        ':1 % 43'
        """
        return CompositeTerm("{} % {}", self, other)

    # Overload standard Python operators where it makes sense
    __lt__ = lt
    __le__ = le
    __eq__ = eq
    __ne__ = ne
    __gt__ = gt
    __ge__ = ge
    __add__ = add
    __sub__ = sub
    __mul__ = mul
    __truediv__ = div
    __floordiv__ = div
    __mod__ = mod
    __or__ = or_
    __and__ = and_

    # Also overload the right associative versions with a simple flip
    _flip = lambda func: lambda a, b: func(b, a)
    __radd__ = _flip(add)
    __rsub__ = _flip(sub)
    __rmul__ = _flip(mul)
    __rtruediv__ = _flip(div)
    __rfloordiv = _flip(div)
    __rmod__ = _flip(mod)
    __ror__ = _flip(or_)
    __rand__ = _flip(and_)


class Column(OperatorTerm):
    """
    Represents column references in a SQL query.

    Examples:
    >>> table = Table('mytable')
    >>> ctx = Context()
    >>> table.foo
    <squibbler.Column 'mytable.foo'>
    >>> table.bar
    <squibbler.Column 'mytable.bar'>
    >>> table.foo is table.foo
    True
    >>> Column('foo').sql(ctx)
    'foo'
    >>> Column('foo', table).sql(ctx)
    'mytable.foo'
    """

    def __init__(self, name: str, table: Table | None = None):
        """Create a `Column` with name `name` and table `Table`."""
        self._name = name
        self._table = table

    def __repr__(self):
        """Return a string representation of this `Column`."""
        if self._table:
            return f"<{self.__module__}.{self.__class__.__name__} {repr(self._table._name + '.' + self._name)}>"
        return f"<{self.__module__}.{self.__class__.__name__} {repr(self._name)}>"

    def __hash__(self):
        """
        Return a hash for this `Column`. Required for `Column` to be used as
        dict keys in `UpdateQuery` values.
        """
        if self._table:
            return hash((self._table._name, self._name))
        return hash(self._name)

    def sql(self, ctx: Context) -> str:
        """Return this term as SQL with the given `ctx`."""
        if (
            self._table
            and isinstance(self._table, Table)
            and ctx.QUERY_MODE != "INSERT"
        ):
            return f"{self._table._alias or self._table._name}.{self._name}"
        return self._name

    def set(self, value: RawType) -> dict[Column, Term]:
        """
        Return a dict with this `Column` mapped to `value`, to be used with `UpdateQuery`.

        Examples:
        >>> Column('foo').set(1)
        {<squibbler.Column 'foo'>: <squibbler.Parameter 1>}
        """
        return {self: wrap_operand(value)}


class CompositeTerm(OperatorTerm):
    """
    Represents a named composite term that can include multiple other terms.
    The formatting of the term depends on the `Context` class that is used to
    compile it.

    Examples:
    >>> CompositeTerm("{} is before {}", Literal(1), Literal(2)).sql(Context())
    '1 is before 2'
    >>> CompositeTerm("{} is before {}", CompositeTerm("{} is before {}", Literal(1), Literal(2)), Literal(2)).sql(Context())
    '1 is before 2 is before 2'
    """

    def __init__(self, formatstr: str, *terms: list[Term]):
        """
        Create a `CompositeTerm` term with the given `name` whose format
        string references `terms`.
        """
        self._formatstr = formatstr
        self._terms = terms

    def __repr__(self):
        """Return a string representation of this `CompositeTerm`."""
        return f"<{self.__module__}.{self.__class__.__name__} {repr(self._formatstr)}>"

    def sql(self, ctx: Context) -> str:
        """Return this term as SQL with the given `ctx`."""
        formatstr = ctx.DIALECT_OPERATORS.get(self._formatstr, self._formatstr)
        args = [term.sql(ctx) for term in self._terms]
        return formatstr.format(*args)

    def group(self) -> CompositeTerm:
        """Apply parantheses around this term, if needed.

        Examples:
        >>> Literal(42).group().group().sql(Context())
        '(42)'
        """
        if self._formatstr == "({})":
            return self
        return super().group()


class JoinedTerm(OperatorTerm):
    """
    Represents a composite term that can include multiple other terms separated
    by a fixed string value.

    Examples:
    >>> JoinedTerm([]).sql(Context())
    ''
    >>> JoinedTerm([Literal(1)]).sql(Context())
    '1'
    >>> JoinedTerm([Literal(1), Literal(2)]).sql(Context())
    '1, 2'
    >>> JoinedTerm([Literal(1), Literal(2), Literal(3)], separator="010").sql(Context())
    '101020103'
    """

    def __init__(self, terms: list[OperatorTerm], separator: str = ", "):
        """Create a `JoinedTerm` with the given `terms` and `separator`."""
        self._terms = terms
        self._separator = separator

    def sql(self, ctx: Context) -> str:
        """Return this term as SQL with the given `ctx`."""
        return self._separator.join(term.sql(ctx) for term in self._terms)


class ConditionalTerm(JoinedTerm):
    """
    Represents a group of conditional terms joined by either AND or OR.

    Examples:
    >>> ConditionalTerm([]).sql(Context())
    ''
    >>> ConditionalTerm([Literal(1) < Literal(2)]).sql(Context())
    '1 < 2'
    >>> ConditionalTerm([Literal(1) < Literal(2), Parameter(True)]).sql(Context())
    '1 < 2 AND :1'
    >>> ConditionalTerm([Literal(1) < Literal(2), Literal(True)], True).sql(Context())
    '1 < 2 OR 1'
    """

    def __init__(
        self,
        terms: list[OperatorTerm],
        isor: bool = False,
        ctx: Context[str, RawType] | None = None,
    ):
        """
        Create a `ConditionalTerm` with the given `terms`. If `isor` is `True` use
        OR separator, otherwise AND.
        """
        if len(terms) > 1:
            terms = [
                term.group() if isinstance(term, ConditionalTerm) and len(term._terms) > 1 else term
                for term in terms
            ]
        super().__init__(terms, " OR " if isor else " AND ")
        self._context = ctx

    def sql(self, ctx: Context) -> str:
        """Return this term as SQL with the given `ctx`."""
        if self._context:
            ctx.update(self._context)
        return super().sql(ctx)


class All(ConditionalTerm):
    """
    Represents a group of conditional terms joined by AND.

    >>> table = Table('foo')
    >>> table.select(table.value).where(All(table.name.startswith("bar"), table.value > 42)).compile()
    ('SELECT foo.value FROM foo WHERE foo.name LIKE :1 AND foo.value > :2', {'1': 'bar%', '2': 42})
    """

    def __init__(self, *terms: list[OperatorTerm], ctx: Context[str, RawType] | None = None):
        super().__init__(terms, isor=False, ctx=ctx)


class Any(ConditionalTerm):
    """
    Represents a group of conditional terms joined by OR.

    >>> table = Table('foo')
    >>> table.select(table.value).where(
    ...     Any(
    ...         table.name.startswith("bar"),
    ...         All(table.value > 42, table.value < 84)
    ...     )
    ... ).compile()
    ('SELECT foo.value FROM foo WHERE foo.name LIKE :1 OR (foo.value > :2 AND foo.value < :3)', {'1': 'bar%', '2': 42, '3': 84})
    """

    def __init__(self, *terms: list[OperatorTerm], ctx: Context[str, RawType] | None = None):
        super().__init__(terms, isor=True, ctx=ctx)


class Parameter(OperatorTerm):
    """
    Represents parameterized values in a SQL query. It is resolved into a
    named parameter and the value is stored in the compilation context for
    use in parameterized query.

    Examples:
    >>> ctx = Context()
    >>> Parameter(42).sql(ctx)
    ':1'
    >>> ctx['1']
    42
    """

    def __init__(self, value: RawType):
        """Create a `Parameter` of the given `value`."""
        self._value = value

    def __repr__(self):
        """Return a string representation of this `Parameter`."""
        return f"<{self.__module__}.{self.__class__.__name__} {repr(self._value)}>"

    def sql(self, ctx: Context) -> str:
        """Return this term as SQL with the given `ctx`."""
        return ctx.resolve_param(self._value)


class Literal(OperatorTerm):
    """
    Represents literal values in a SQL query. These are values that should
    *not* be turned into named parameters in the SQL query, but may be casted
    into SQL equivalents and escaped as needed.

    Examples:
    >>> Literal('foo')
    <squibbler.Literal 'foo'>
    >>> Literal("'foo'")
    <squibbler.Literal '''foo'''>
    >>> Literal(42)
    <squibbler.Literal 42>
    >>> Literal(None)
    <squibbler.Literal NULL>
    """

    def __init__(self, value: RawType):
        """Create a `Literal` of the given `value`."""
        self._value = value

    def __repr__(self):
        """Return a string representation of this `Literal`."""
        return f"<{self.__module__}.{self.__class__.__name__} {Context.resolve_literal(self._value)}>"

    def __hash__(self):
        """
        Return a hash for this `Literal`. Required for `Literal` to be used as
        dict keys in `UpdateQuery` values.
        """
        return hash(self._value)

    def sql(self, ctx: Context) -> str:
        """
        Return this term as SQL with the given `ctx`.

        Examples:
        >>> Literal('foo').sql(Context())
        "'foo'"
        >>> Literal("'foo'").sql(Context())
        "'''foo'''"
        >>> Literal(42).sql(Context())
        '42'
        >>> Literal(None).sql(Context())
        'NULL'
        >>> (Literal(1) < Literal(2)).sql(Context())
        '1 < 2'
        """
        return ctx.resolve_literal(self._value)


class RawSql(OperatorTerm):
    """
    Represents raw SQL values in a query. These will be resolved as-is without
    any casting other than to `str`.

    >>> RawSql('42').sql(Context())
    '42'
    >>> RawSql(42).sql(Context())
    '42'
    """

    def __init__(self, value: RawType):
        """Create a `RawSql` object with the given `value`."""
        self._value = value

    def sql(self, ctx: Context) -> str:
        """Return this term as SQL with the given `ctx`."""
        return str(self._value)


class Query(OperatorTerm):
    """Base class for all types of SQL queries."""

    column_cls = Column
    context_cls = Context
    separator = " "

    def __init__(self, table: Term | None = None, connection: Protocol | None = None):
        """Create a `Query` for the given `table`."""
        if isinstance(table, Query):
            table = table.group()
        self._table: Term | None = table
        self._conn: Protocol | None = connection
        if self._conn is None and self._table and isinstance(self._table, Table):
            self._conn = self._table._conn
        self._where: ConditionalTerm | None = None
        self._joins: list[tuple[str, Table, AnyTerm]] = []

    def compile(self) -> tuple[str, Context]:
        """Return the SQL and bound parameters for this `Query`."""
        ctx = self.context_cls()
        return (self.sql(ctx), ctx)

    def compilesql(self) -> str:
        """Return the SQL for this `Term`."""
        return self.compile()[0]

    def alias(self, alias: str) -> CompositeTerm:
        """Apply a name to this `Query`."""
        return CompositeTerm(f"({{term}}) AS {alias}", term=self)

    def where(self, *terms: AnyTerms, **params: dict[str, RawType]) -> Query:
        """
        Add terms to the WHERE clause of this query. Multiple terms are joined
        by AND logic.

        The API of `Query.where` can be broken down into three different use
        patterns.

        1) Simple equality using only the `params` keyword arguments: Every
        key-value pair is treated as an equality conditional with the key as a
        column name. The values are parameterized.

        >>> table = Table('mytable')
        >>> table.select().where(id=42).compile()
        ('SELECT * FROM mytable WHERE mytable.id = :1', {'1': 42})
        >>> table.select().where(id=42, name='Zaphod').compile()
        ('SELECT * FROM mytable WHERE mytable.id = :1 AND mytable.name = :2', {'1': 42, '2': 'Zaphod'})

        2) `terms` consisting of `Term` objects, typically created using
        operations with `Column` objects. With this pattern the `params`
        keyword arguments are not needed.

        >>> table.select().where(table.id == 42).compile()
        ('SELECT * FROM mytable WHERE mytable.id = :1', {'1': 42})
        >>> table.select().where(table.id == 42, table.name == 'Zaphod').compile()
        ('SELECT * FROM mytable WHERE mytable.id = :1 AND mytable.name = :2', {'1': 42, '2': 'Zaphod'})
        >>> table.select().where(table.id.in_([42, 43, 44])).compile()
        ('SELECT * FROM mytable WHERE mytable.id IN (:1, :2, :3)', {'1': 42, '2': 43, '3': 44})

        3) `terms` consisting of `str` objects for custom snippets of SQL code.
        With this pattern the `params` keyword arguments can be used to provide
        parameterized values.

        >>> table.select().where(True).compile()
        ('SELECT * FROM mytable WHERE 1', {})
        >>> table.select().where('id in (SELECT something IN myothertable where id=:id)', id=42).compile()
        ('SELECT * FROM mytable WHERE id in (SELECT something IN myothertable where id=:id)', {'id': 42})
        """
        return self._add_where(terms, False, params)

    def orwhere(self, *terms: AnyTerms, **params: dict[str, RawType]) -> Query:
        """
        Add an `OR` clause to the existing `WHERE` clause. Otherwise same as `Query.where`.

        >>> table = Table("mytable")
        >>> table.select().where(foo=42, bar=-1).orwhere(fizz=84, fuzz=84).compilesql()
        'SELECT * FROM mytable WHERE (mytable.foo = :1 AND mytable.bar = :2) OR (mytable.fizz = :3 AND mytable.fuzz = :4)'
        >>> table.select().orwhere(foo=42).orwhere(fizz=84).compilesql()
        'SELECT * FROM mytable WHERE mytable.foo = :1 OR mytable.fizz = :2'
        >>> table.select().orwhere(foo=42).orwhere(fizz=84, fuzz=84).compilesql()
        'SELECT * FROM mytable WHERE mytable.foo = :1 OR (mytable.fizz = :2 AND mytable.fuzz = :3)'
        >>> table.select().orwhere(foo=42).orwhere(fizz=84, fuzz=84).where(bar=0).compilesql()
        'SELECT * FROM mytable WHERE (mytable.foo = :1 OR (mytable.fizz = :2 AND mytable.fuzz = :3)) AND mytable.bar = :4'
        """
        return self._add_where(terms, True, params)

    def join(
        self, jointable: Table | Query, *terms: AnyTerms, **params: dict[str, RawType]
    ) -> Query:
        """
        Add a JOIN clause with `jointable`.

        If `terms` are given they form the ON conditional for the join. As with
        `Query.where` the `terms` can be either `Term` objects or plain `str`
        objects for custom SQL, when needed. Multiple terms are joined by AND
        logic.

        As with `Query.where` the `params` keyword arguments can be used to
        provide parameterized values when using custom SQL snippets.

        Examples:
        >>> table1 = Table('table1')
        >>> table2 = Table('table2')
        >>> table1.select().join(table2).compile()
        ('SELECT * FROM table1 JOIN table2', {})
        >>> table1.select().join(table2, table1.id == table2.id).compile()
        ('SELECT * FROM table1 JOIN table2 ON table1.id = table2.id', {})
        >>> table1.select().join(table2, (table1.id == table2.id).or_(table1.id2 == table2.id2)).compile()
        ('SELECT * FROM table1 JOIN table2 ON (table1.id = table2.id) OR (table1.id2 = table2.id2)', {})
        >>> table1.select().join(table2, 'table1.foo = :foo', foo=42).compile()
        ('SELECT * FROM table1 JOIN table2 ON table1.foo = :foo', {'foo': 42})

        Sub-query example:
        >>> query = table2.select().where(table2.foo % 2)
        >>> table1.select().join(query, table1.foo == table2.foo).compile()
        ('SELECT * FROM table1 JOIN (SELECT * FROM table2 WHERE table2.foo % :1) ON table1.foo = table2.foo', {'1': 2})
        """
        return self._add_join("JOIN", jointable, terms, params)

    def innerjoin(
        self, jointable: Table | Query, *terms: AnyTerms, **params: dict[str, RawType]
    ) -> Query:
        """Same as `Query.join` but produces an INNER JOIN instead."""
        return self._add_join("INNER JOIN", jointable, terms, params)

    def outerjoin(
        self, jointable: Table | Query, *terms: AnyTerms, **params: dict[str, RawType]
    ) -> Query:
        """Same as `Query.join` but produces an OUTER JOIN instead."""
        return self._add_join("OUTER JOIN", jointable, terms, params)

    def leftjoin(
        self, jointable: Table | Query, *terms: AnyTerms, **params: dict[str, RawType]
    ) -> Query:
        """Same as `Query.join` but produces a LEFT JOIN instead."""
        return self._add_join("LEFT JOIN", jointable, terms, params)

    def rightjoin(
        self, jointable: Table | Query, *terms: AnyTerms, **params: dict[str, RawType]
    ) -> Query:
        """Same as `Query.join` but produces a RIGHT JOIN instead."""
        return self._add_join("RIGHT JOIN", jointable, terms, params)

    def _add_where(
        self, terms: AnyTerms, isor: bool, params: dict[str, RawType]
    ) -> Query:
        if terms:
            terms = self._wrap_args(terms)
            conditional = ConditionalTerm(terms, isor, params)
        elif params:
            terms = [
                self.column_cls(name, self._table).eq(value)
                for name, value in params.items()
            ]
            conditional = ConditionalTerm(terms)
        else:
            return self
        if self._where:
            conditional = ConditionalTerm([self._where, conditional], isor)
        self._where = conditional
        return self

    def _add_join(
        self,
        jointype: str,
        jointable: Term,
        terms: AnyTerms,
        params: dict[str, RawType],
    ) -> Query:
        joincond = (
            ConditionalTerm(self._wrap_args(terms), False, params) if terms else None
        )
        self._joins.append((jointype, self._wrap_arg(jointable), joincond))
        return self

    def _wrap_args(self, terms: AnyTerms) -> Term:
        """
        Return a copy of `terms` with all non-`Term` items wrapped as some
        sort of `Term` object based on its type.
        """
        return [self._wrap_arg(term) for term in terms]

    @staticmethod
    def _wrap_arg(term: AnyTerm) -> Term:
        """
        Return some sort of `Term` object that wraps `term` based on its
        type.
        """
        if isinstance(term, Query):
            return term.group()
        if isinstance(term, Term):
            return term
        if isinstance(term, str):
            return RawSql(term)
        return Literal(term)

    ### DB API METHODS ###

    def execute(self) -> Protocol:
        assert self._conn, "Cannot execute without a DB API connection"
        sql, ctx = self.compile()
        cursor = self._conn.cursor()
        cursor.execute(sql, ctx)
        return cursor

    def fetchmany(self, *args, **kwargs) -> Any:
        cursor = self.execute()
        return cursor.fetchmany(*args, **kwargs)

    def fetchall(self) -> Any:
        cursor = self.execute()
        return cursor.fetchall()


class SelectQuery(Query):
    """Represents a SELECT query."""

    def __init__(self, table: Term = None):
        """Create a `SelectQuery` for the given `table`."""
        super().__init__(table)
        self._select: AnyTerms = []
        self._distinct: bool = False
        self._groupby: AnyTerms = []
        self._orderby: AnyTerms = []
        self._limit: int = 0
        self._offset: int = 0

    def sql(self, ctx: Context) -> str:
        """Return this query as SQL with the given `ctx`."""
        parts = []
        parts.append("SELECT")
        if self._distinct:
            parts.append("DISTINCT")
        if self._select:
            parts.append(", ".join(term.sql(ctx) for term in self._select))
        else:
            parts.append("*")
        if self._table:
            parts.append("FROM")
            parts.append(self._table.sql(ctx))
        for jointype, jointable, joincond in self._joins:
            parts.append(jointype)
            parts.append(jointable.sql(ctx))
            if joincond:
                parts.append("ON")
                parts.append(joincond.sql(ctx))
        if self._where:
            parts.append("WHERE")
            parts.append(self._where.sql(ctx))
        if self._groupby:
            parts.append("GROUP BY")
            parts.append(", ".join(group.sql(ctx) for group in self._groupby))
        if self._orderby:
            parts.append("ORDER BY")
            parts.append(", ".join(order.sql(ctx) for order in self._orderby))
        if self._limit:
            parts.append("LIMIT")
            parts.append(self._limit.sql(ctx))
        if self._offset:
            parts.append("OFFSET")
            parts.append(self._offset.sql(ctx))
        return self.separator.join(parts)

    def subselect(self, *terms: AnyTerms) -> SelectQuery:
        """
        Return a new `SelectQuery` that selects from this query instead of a
        table.

        Examples:
        >>> query = Table('mytable').select()
        >>> query.subselect().compilesql()
        'SELECT * FROM (SELECT * FROM mytable)'
        >>> query.where(foo=42)
        <squibbler.SelectQuery ...>
        >>> query.subselect().compilesql()
        'SELECT * FROM (SELECT * FROM mytable WHERE mytable.foo = :1)'
        >>> query.subselect().where(bar=42).compilesql()
        'SELECT * FROM (SELECT * FROM mytable WHERE mytable.foo = :1) WHERE bar = :2'
        >>> query.subselect().join(Table("myothertable")).compilesql()
        'SELECT * FROM (SELECT * FROM mytable WHERE mytable.foo = :1) JOIN myothertable'
        """
        return self.__class__(self).select(*terms)

    def select(self, *terms: AnyTerms) -> SelectQuery:
        """
        Add `terms` to the query selection.

        >>> SelectQuery().select().compilesql()
        'SELECT *'
        >>> SelectQuery().select(1).compilesql()
        'SELECT 1'
        >>> SelectQuery().select('foo').compilesql()
        'SELECT foo'
        >>> SelectQuery().select(Literal('foo')).compilesql()
        "SELECT 'foo'"
        >>> SelectQuery().select(Parameter('foo')).compilesql()
        'SELECT :1'
        >>> SelectQuery().select('foo', 'bar').compilesql()
        'SELECT foo, bar'
        >>> SelectQuery().select('foo').select('bar').compilesql()
        'SELECT foo, bar'
        """
        self._select.extend(self._wrap_args(terms))
        return self

    def distinct(self, flag: bool = True) -> SelectQuery:
        """
        Set the DISTINCT flag.

        >>> SelectQuery().select().distinct().compilesql()
        'SELECT DISTINCT *'
        """
        self._distinct = flag
        return self

    def groupby(self, *terms: AnyTerms) -> SelectQuery:
        """
        Add a GROUP BY clause.

        >>> SelectQuery().select().groupby('foo').compilesql()
        'SELECT * GROUP BY foo'
        >>> SelectQuery().select().groupby('foo', 'bar').compilesql()
        'SELECT * GROUP BY foo, bar'
        >>> SelectQuery().select().groupby(Table('mytable').foo == 42).compilesql()
        'SELECT * GROUP BY mytable.foo = :1'
        """
        self._groupby.extend(self._wrap_args(terms))
        return self

    def orderby(self, *terms: AnyTerms) -> SelectQuery:
        """
        Add an ORDER BY clause.

        >>> SelectQuery().select().orderby('foo').compilesql()
        'SELECT * ORDER BY foo'
        >>> SelectQuery().select().orderby('foo', 'bar').compilesql()
        'SELECT * ORDER BY foo, bar'
        >>> SelectQuery().select().orderby(Table('mytable').foo == 42).compilesql()
        'SELECT * ORDER BY mytable.foo = :1'
        """
        self._orderby.extend(self._wrap_args(terms))
        return self

    def limit(self, n: AnyTerm) -> SelectQuery:
        """
        Add a LIMIT clause.

        >>> SelectQuery().select().limit(42).compilesql()
        'SELECT * LIMIT 42'
        """
        self._limit = self._wrap_arg(n)
        return self

    def offset(self, n: int) -> SelectQuery:
        """
        Add an OFFSET clause.

        >>> SelectQuery().select().offset(24).compilesql()
        'SELECT * OFFSET 24'
        >>> SelectQuery().select().offset(24).limit(42).compilesql()
        'SELECT * LIMIT 42 OFFSET 24'
        """
        self._offset = self._wrap_arg(n)
        return self


class UpdateQuery(Query):
    """
    Represents an UPDATE query.

    >>> table = Table("mytable")
    >>> table.update(foo=42).compile()
    ('UPDATE mytable SET mytable.foo = :1', {'1': 42})
    >>> table.update(foo=42, bar=32).compile()
    ('UPDATE mytable SET mytable.foo = :1, mytable.bar = :2', {'1': 42, '2': 32})
    >>> table.update(table.foo.set(42), table.bar.set(32)).compile()
    ('UPDATE mytable SET mytable.foo = :1, mytable.bar = :2', {'1': 42, '2': 32})
    >>> table.update(foo=42).where(bar=23).compile()
    ('UPDATE mytable SET mytable.foo = :1 WHERE mytable.bar = :2', {'1': 42, '2': 23})
    """

    def __init__(self, table: Table, values: dict[Term, Term]):
        super().__init__(table)
        self._values: dict[str, AnyTerm] = values

    @classmethod
    def from_values(
        cls,
        table: Table,
        args: list[dict[AnyTerm, AnyTerm]],
        kwargs: dict[str, AnyTerm],
    ) -> UpdateQuery:
        for arg in args:
            kwargs.update(arg)
        kwargs = {
            key if isinstance(key, Column) else table[key]: wrap_operand(value)
            for key, value in kwargs.items()
        }
        return cls(table, kwargs)

    def sql(self, ctx: Context) -> str:
        """Return this query as SQL with the given `ctx`."""
        parts = []
        parts.append("UPDATE")
        parts.append(self._table.sql(ctx))
        parts.append("SET")
        parts.append(
            ", ".join(
                f"{key.sql(ctx)} = {value.sql(ctx)}"
                for key, value in self._values.items()
            )
        )
        if self._where:
            parts.append("WHERE")
            parts.append(self._where.sql(ctx))
        return self.separator.join(parts)


class InsertQuery(UpdateQuery):
    """
    Represents an INSERT query.

    >>> table = Table("mytable")
    >>> table.insert(foo=42).compile()
    ('INSERT INTO mytable (foo) VALUES (:1)', {'1': 42})
    >>> table.insert(foo=42, bar=23).compile()
    ('INSERT INTO mytable (foo, bar) VALUES (:1, :2)', {'1': 42, '2': 23})
    >>> table.insert(table.foo.set(23)).compile()
    ('INSERT INTO mytable (foo) VALUES (:1)', {'1': 23})
    """

    def sql(self, ctx: Context) -> str:
        """Return this query as SQL with the given `ctx`."""
        ctx.QUERY_MODE = "INSERT"
        parts = []
        parts.append("INSERT INTO")
        parts.append(self._table.sql(ctx))
        parts.append("(" + ", ".join(key.sql(ctx) for key in self._values.keys()) + ")")
        parts.append("VALUES")
        parts.append(
            "(" + ", ".join(term.sql(ctx) for term in self._values.values()) + ")"
        )
        return self.separator.join(parts)


class DeleteQuery(Query):
    """
    Represents a DELETE query.

    >>> table = Table("mytable")
    >>> table.delete().compile()
    ('DELETE FROM mytable', {})
    >>> table.delete().where(foo=42).compile()
    ('DELETE FROM mytable WHERE mytable.foo = :1', {'1': 42})
    >>> table.delete().where(table.bar < 42).compile()
    ('DELETE FROM mytable WHERE mytable.bar < :1', {'1': 42})
    """

    def sql(self, ctx: Context) -> str:
        """Return this query as SQL with the given `ctx`."""
        parts = []
        parts.append("DELETE FROM")
        parts.append(self._table.sql(ctx))
        if self._where:
            parts.append("WHERE")
            parts.append(self._where.sql(ctx))
        return self.separator.join(parts)


class Table(Term):
    """
    Represents a SQL table. Used as a factory for `Column` and `Query` objects.

    Examples:
    >>> table = Table("mytable")
    >>> table.foo
    <squibbler.Column 'mytable.foo'>
    >>> table.select(table.foo, table.bar)
    <squibbler.SelectQuery ...>
    >>> table.insert(foo=42, bar=24)
    <squibbler.InsertQuery ...>
    >>> table.update(foo=43)
    <squibbler.UpdateQuery ...>
    """

    select_cls = SelectQuery
    insert_cls = InsertQuery
    update_cls = UpdateQuery
    delete_cls = DeleteQuery

    def __init__(
        self, name: str, alias: str | None = None, connection: Protocol | None = None
    ):
        """
        Create a `Table` with name `name`. If `alias` is given, this will be
        used as shorthand in queries.
        """
        self._name = name
        self._alias = alias
        self._conn = connection
        self._columns = {}

    def __repr__(self):
        """Return a string representation of this `Table`."""
        return f"<{self.__module__}.{self.__class__.__name__} {repr(self._name)}>"

    def __getattr__(self, attr: str):
        """If `attr` does not exist, instead return a `Column` for this `Table`."""
        try:
            return super().__getattr__(attr)
        except AttributeError:
            return self[attr]

    def __getitem__(self, item: str):
        """Return a column for this `Table`."""
        try:
            return self._columns[item]
        except KeyError:
            self._columns[item] = self.select_cls.column_cls(item, self)
        return self._columns[item]

    def sql(self, ctx: Context) -> str:
        """Return this term as SQL for the given `ctx`."""
        if self._alias:
            return f"{self._name} AS {self._alias}"
        return self._name

    def select(self, *terms: AnyTerms) -> SelectQuery:
        """
        Return a `SelectQuery` for this table with `SelectQuery.select` called with `terms`.

        >>> table = Table("mytable")
        >>> table.select().compilesql()
        'SELECT * FROM mytable'
        >>> table.select(table.foo).compilesql()
        'SELECT mytable.foo FROM mytable'
        >>> table.select(table.foo, table.bar).compilesql()
        'SELECT mytable.foo, mytable.bar FROM mytable'
        """
        return self.select_cls(self).select(*terms)

    def update(
        self, *args: list[dict[Column, AnyTerm]], **kwargs: dict[str, AnyTerm]
    ) -> UpdateQuery:
        """Return an `UpdateQuery` for this table."""
        return self.update_cls.from_values(self, args, kwargs)

    def insert(
        self, *args: list[dict[Column, AnyTerm]], **kwargs: dict[str, AnyTerm]
    ) -> InsertQuery:
        """Return an `InsertQuery` for this table."""
        return self.insert_cls.from_values(self, args, kwargs)

    def delete(self) -> DeleteQuery:
        """Return a `DeleteQuery` for this table."""
        return self.delete_cls(self)


class Database:
    """
    Represents a SQL database and distributes a provided DB-API
    connection to all tables and queries created from it. Used as
    a factory for `Table` objects.

    Examples:
    >>> import sqlite3
    >>> conn = sqlite3.connect(":memory:")
    >>> conn.execute("CREATE TABLE foo(key TEXT, val TEXT)")
    <sqlite3.Cursor object ...>
    >>> conn.commit()
    >>> db = Database(conn)
    >>> db.foo
    <squibbler.Table 'foo'>
    >>> db.foo is db.foo
    True
    >>> db.foo.insert(key="foo", val="42").execute()
    <sqlite3.Cursor object ...>
    >>> db.foo.insert(key="bar", val="43").execute()
    <sqlite3.Cursor object ...>
    >>> db.foo.insert({"key": "foobar", "val": "44"}).execute()
    <sqlite3.Cursor object ...>
    >>> db.foo.select().fetchall()
    [('foo', '42'), ('bar', '43'), ('foobar', '44')]
    >>> db.foo.select(db.foo.val).where(key="bar").fetchall()
    [('43',)]
    >>> db.foo.select(db.foo.val).where(db.foo.key.contains("oba")).fetchall()
    [('44',)]
    """

    table_cls = Table

    def __init__(self, connection: Protocol):
        """
        Create a `Database` using the given DB-API connection.
        """
        self._conn = connection
        self._tables = {}

    def __getattr__(self, attr: str):
        """If `attr` does not exist, instead return a `Table` for this `Database`."""
        try:
            return super().__getattr__(attr)
        except AttributeError:
            return self[attr]

    def __getitem__(self, item: str):
        """Return a `Table` for this `Database`."""
        try:
            return self._tables[item]
        except KeyError:
            self._tables[item] = self.table_cls(item, connection=self._conn)
        return self._tables[item]
