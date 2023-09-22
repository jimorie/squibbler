# Squibble

A simple SQL query builder for Python.

#### Design goals

The aim with Squibble is to provide a fast SQL query builder for Python with an intuitive API. Performance is prioritized over completeness or nice-to-have properties, such as immutability.

In fact, immutability is the main difference from the competing library [Pypika](https://github.com/kayak/pypika). Because Pypika guarantees immutability while Squibble does not, Squibble is able to benchmark better performance. In all other aspects Pypika is a better and much more complete and mature library.

## Tables

`Table` objects are the normal starting points when building a query.

```python
>>> from squibble import *
>>> Table('mytable')
<squibble.Table 'mytable'>

```

### Table Aliases

SQL labels for `Table` objects can be used by specifying a second argument.

```python
>>> table = Table('mytable', 'f')
>>> table.select(table.foo).compile()
('SELECT f.foo FROM mytable AS f', {})

```

## Queries

`Query` objects can be created by using the `Table.select`, `Table.insert`, `Table.update` or `Table.delete` methods. These methods return a new instance of the corresponding `Query` object that references the creating `Table` object.

`Query` objects can then be manipulated using a number of methods to create the desired SQL query.

Note that `Query` objects are mutable. Their class methods typically return `self` to allow for command chaining, but this should not be mistaken for immutability.

Finally the `Query.compile` method can be called to construct the SQL query string and the bound parameters.

### Select Queries

`SelectQuery` objects are typically created with the `Table.select` method.

```python
>>> table = Table('mytable')
>>> table.select(table.foo).compile()
('SELECT mytable.foo FROM mytable', {})

```

#### Selection Aliases

SQL labels for selected objects can be created with the `Term.alias` method.

```python
>>> table.select(table.foo.alias('bar')).compile()
('SELECT mytable.foo AS bar FROM mytable', {})

```

#### DISTINCT

```python
>>> table.select(table.foo).distinct().compile()
('SELECT DISTINCT mytable.foo FROM mytable', {})

```

#### GROUP BY

```python
>>> table.select().groupby(table.foo).compile()
('SELECT * FROM mytable GROUP BY mytable.foo', {})

```

#### ORDER BY

```python
>>> table.select().orderby(table.foo).compile()
('SELECT * FROM mytable ORDER BY mytable.foo', {})
>>> table.select().orderby(table.foo, table.bar.desc()).compile()
('SELECT * FROM mytable ORDER BY mytable.foo, mytable.bar DESC', {})

```

#### LIMIT and OFFSET

```python
>>> table.select().limit(1).compile()
('SELECT * FROM mytable LIMIT 1', {})
>>> table.select().limit(1).offset(42).compile()
('SELECT * FROM mytable LIMIT 1 OFFSET 42', {})

```

### Insert Queries

`InsertQuery` objects are typically created with the `Table.insert` method.

```python
>>> table.insert(foo=42).compile()
('INSERT INTO mytable (mytable.foo) VALUES (:1)', {'1': 42})

```

### Update Queries

`UpdateQuery` objects are typically created with the `Table.update` method.

```python
>>> table.update(foo=42).compile()
('UPDATE mytable SET mytable.foo = :1', {'1': 42})

```

Both `Table.insert` and `Table.update` can also take `dict` arguments with the values.

```python
>>> table.update({'foo': 42}).compile()
('UPDATE mytable SET mytable.foo = :1', {'1': 42})
>>> table.update({table.foo: 42}).compile()
('UPDATE mytable SET mytable.foo = :1', {'1': 42})

```

### Where Clauses

`WHERE` clauses are added with the `Query.where` method, whose usage can be broken down into three different use patterns.

1. Simple equality using only keyword arguments: Every key-value pair is treated as an equality conditional with the key as a column name. The values are parameterized.

    ```python
    >>> table.select().where(id=42).compile()
    ('SELECT * FROM mytable WHERE mytable.id = :1', {'1': 42})
    >>> table.select().where(id=42, name='Zaphod').compile()
    ('SELECT * FROM mytable WHERE mytable.id = :1 AND mytable.name = :2', {'1': 42, '2': 'Zaphod'})

    ```

2. `Term` objects as positional arguments, typically created using operations with `Column` objects. With this pattern keyword arguments are not needed.

    ```python
    >>> table.select().where(table.id == 42).compile()
    ('SELECT * FROM mytable WHERE mytable.id = :1', {'1': 42})
    >>> table.select().where(table.id == 42, table.name == 'Zaphod').compile()
    ('SELECT * FROM mytable WHERE mytable.id = :1 AND mytable.name = :2', {'1': 42, '2': 'Zaphod'})
    >>> table.select().where(table.id.in_([42, 43, 44])).compile()
    ('SELECT * FROM mytable WHERE mytable.id IN (:1, :2, :3)', {'1': 42, '2': 43, '3': 44})

    ```

3. Non-`Term` (typically `str`) objects as positional arguments for custom snippets of SQL code. With this pattern keyword arguments can be used to provide parameterized values.

    ```python
    >>> table.select().where(True).compile()
    ('SELECT * FROM mytable WHERE 1', {})
    >>> table.select().where('id in (SELECT something IN myothertable where id=:id)', id=42).compile()
    ('SELECT * FROM mytable WHERE id in (SELECT something IN myothertable where id=:id)', {'id': 42})

    ```

#### OR

Repeated calls to `Query.where` joins new conditions to the old conditions using `AND`. To instead join new conditions to the old conditions using `OR`, instead use `Query.orwhere`. Note that both `Query.where` and `Query.orwhere` joins multiple conditions from the same call using `AND`.

```python
>>> table.select().where(foo=42, bar=-1).orwhere(fizz=84, fuzz=84).compilesql()
'SELECT * FROM mytable WHERE (mytable.foo = :1 AND mytable.bar = :2) OR (mytable.fizz = :3 AND mytable.fuzz = :4)'
>>> table.select().orwhere(foo=42).orwhere(fizz=84).compilesql()
'SELECT * FROM mytable WHERE mytable.foo = :1 OR mytable.fizz = :2'
>>> table.select().orwhere(foo=42).orwhere(fizz=84, fuzz=84).compilesql()
'SELECT * FROM mytable WHERE mytable.foo = :1 OR (mytable.fizz = :2 AND mytable.fuzz = :3)'
>>> table.select().orwhere(foo=42).orwhere(fizz=84, fuzz=84).where(bar=0).compilesql()
'SELECT * FROM mytable WHERE (mytable.foo = :1 OR (mytable.fizz = :2 AND mytable.fuzz = :3)) AND mytable.bar = :4'

```

#### `&` and `|`

The `&` and `|` operators can be used to more naturally create advanced expressions with `AND` and `OR`. However, because the operator priorities of the `&` and `|` operators are hard coded by Python, parantheses are usually required to create correct logic.

```python
>>> table.select().where((table.foo == 1) & (table.bar == 2)).compile()
('SELECT * FROM mytable WHERE (mytable.foo = :1) AND (mytable.bar = :2)', {'1': 1, '2': 2})
>>> table.select().where((table.foo == 1) & ((table.bar == 2) | (table.bar == 3))).compile()
('SELECT * FROM mytable WHERE (mytable.foo = :1) AND ((mytable.bar = :2) OR (mytable.bar = :3))', {'1': 1, '2': 2, '3': 3})

```

### Join Clauses

`JOIN` clauses are added with one of the `Query.join` methods. The different methods create different types of join, with support for: `JOIN`, `INNER JOIN`, `OUTER JOIN`, `LEFT JOIN` and `RIGHT JOIN`.

As with `Query.where` the keyword arguments can be used to provide parameterized values when using custom SQL snippets.

```python
>>> table1 = Table('table1')
>>> table2 = Table('table2')
>>> table1.select().join(table2).compile()
('SELECT * FROM table1 JOIN table2', {})
>>> table1.select().join(table2, table1.id == table2.id).compile()
('SELECT * FROM table1 JOIN table2 ON table1.id = table2.id', {})
>>> table1.select().join(table2, (table1.id == table2.id) | (table1.id2 == table2.id2)).compile()
('SELECT * FROM table1 JOIN table2 ON (table1.id = table2.id) OR (table1.id2 = table2.id2)', {})
>>> table1.select().join(table2, 'table1.foo = :foo', foo=42).compile()
('SELECT * FROM table1 JOIN table2 ON table1.foo = :foo', {'foo': 42})

```

### Sub-Queries

`Query` objects can be used as any other term when sub-queries are needed.

E.g. in a select clause:

```python
>>> table1 = Table('table1')
>>> table2 = Table('table2')
>>> query = table1.select(table1.foo).where(table1.id == table2.id)
>>> table2.select(table2.id, query).compile()
('SELECT table2.id, (SELECT table1.foo FROM table1 WHERE table1.id = table2.id) FROM table2', {})

```

A where clause:

```python
>>> table2.select().where(table2.bar == query).compile()
('SELECT * FROM table2 WHERE table2.bar = (SELECT table1.foo FROM table1 WHERE table1.id = table2.id)', {})

```

Or a join clause:

```python
>>> query2 = table2.select().where(table2.foo % 2)
>>> table1.select().join(query2, table1.foo == table2.foo).compile()
('SELECT * FROM table1 JOIN (SELECT * FROM table2 WHERE table2.foo % :1) ON table1.foo = table2.foo', {'1': 2})

```

#### Selecting from sub-query

`Query` objects can also be used instead of a `Table` in the `FROM` clause, using the `SelectQuery.subselect` method.

```python
>>> query = table.select().where(foo=42)
>>> query.subselect(table.bar).where(table.bar == 24).compile()
('SELECT mytable.bar FROM (SELECT * FROM mytable WHERE mytable.foo = :1) WHERE mytable.bar = :2', {'1': 42, '2': 24})

```

Alternatively the `SelectQuery` class can be initialized directly with the `Query` object.

```python
>>> SelectQuery(query).select(table.bar).where(table.bar == 24).compile()
('SELECT mytable.bar FROM (SELECT * FROM mytable WHERE mytable.foo = :1) WHERE mytable.bar = :2', {'1': 42, '2': 24})

```

## Terms

`Term` objects are the individual pieces that are used to build a `Query`. Most `Query` methods accept any type of `Term` objects as arguments.

### Columns

`Column` objects represent column references in a query. These `Term` objects can be created by accessing a non-existing attribute of a `Table` object, or by accessing any item of a `Table` object using standard square brackets syntax.

```python
>>> table = Table('mytable')
>>> table.foo.sql(Context())
'mytable.foo'

```

### Parameters

`Parameter` objects represent parameterized values in a SQL query. These `Term` objects are resolved into a named parameter and their values are stored in the compilation context for use in parameterized query.

```python
>>> ctx = Context()
>>> Parameter(42).sql(ctx)
':1'
>>> ctx
{'1': 42}

```

### Literals

`Literal` objects represent literal values in a query. These `Term` objects are rendered as literal SQL values, and may be casted and escaped as needed.

```python
>>> Literal('foo').sql(Context())
"'foo'"
>>> Literal("'foo'").sql(Context())
"'''foo'''"
>>> Literal(42).sql(Context())
'42'
>>> Literal(None).sql(Context())
'NULL'

```

### Raw SQL

`RawSql` objects represent raw SQL values in a query. These `Term` objects are rendered as-is without any escaping or casting other than to `str`.

```python
>>> RawSql('foo').sql(Context())
'foo'
>>> RawSql("'foo'").sql(Context())
"'foo'"
>>> RawSql(42).sql(Context())
'42'
>>> RawSql(None).sql(Context())
'None'

```

### Composites

`Term` objects can also be a composite of multiple other `Term` objects. These `Term` objects are the result of doing various operations on top-level `Term` objects, e.g `x + y`, `(x, y, z)` or `MAX(x)`. The `CompositeTerm`, `JoinedTerm` and `ConditionalTerm` classes all fall under this category.

```python
>>> (table.x + table.y).sql(Context())
'mytable.x + mytable.y'
>>> table.x.in_([Literal(1), Literal(2), Literal(3)]).sql(Context())
'mytable.x IN (1, 2, 3)'
>>> table.x.max().sql(Context())
'MAX(mytable.x)'

```

### Raw Python values

Raw Python values are automatically converted to either `RawSql` objects or to `Parameter` objects with some basic rules.

When using raw Python values as positional arguments to `Query` methods such as `SelectQuery.select`, `Query.where`, and `Query.join` they are automatically converted to `RawSql` objects. This means you can pass `str` objects to these methods to inject any sort of custom SQL code.

When using raw Python values as operators to other `Term` objects they are automatically converted to `Parameter` objects. This is usually also the case when using raw Python values as keyword arguments to methods such as `Query.where` or `Table.update`.

If you need a different behaviour you cannot use raw Python values but need to pass in the desired type of `Term` object instead.

```python
>>> ctx = Context()
>>> table.select('my bogus SQL').sql(ctx)
'SELECT my bogus SQL FROM mytable'
>>> table.select(table.x == 'my bogus SQL').sql(ctx)
'SELECT mytable.x = :1 FROM mytable'
>>> ctx
{'1': 'my bogus SQL'}

```
