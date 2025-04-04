# Dapper.FSharp [![NuGet](https://img.shields.io/nuget/v/Dapper.FSharp.svg?style=flat-square)](https://www.nuget.org/packages/Dapper.FSharp/)

<p align="center">
<img src="https://github.com/Dzoukr/Dapper.FSharp/raw/master/logo.png" width="150px"/>
</p>

Lightweight F# extension for StackOverflow Dapper with support for MSSQL, MySQL, PostgreSQL and SQLite

## Features

- No *auto-attribute-based-only-author-maybe-knows-magic* behavior
- Support for (anonymous) F# records
- Support for F# options
- LINQ Query Provider
- Support for SQL Server 2012 (11.x) and later / Azure SQL Database, MySQL 8.0, PostgreSQL 12.0, SQLite 3
- Support for SELECT (including JOINs), INSERT, UPDATE (full / partial), DELETE
- Support for OUTPUT clause (MSSQL only)
- Support for INSERT OR REPLACE clause (SQLite)
- Easy usage thanks to F# computation expressions
- Keeps things simple

## Installation
If you want to install this package manually, use usual NuGet package command

    Install-Package Dapper.FSharp

or using [Paket](http://fsprojects.github.io/Paket/getting-started.html)

    paket add Dapper.FSharp

## What's new in v4?
Reasoning behind version 4 is described in [this issue](https://github.com/Dzoukr/Dapper.FSharp/issues/71), but the main changes are:

- Each database provider has its own query definition
- New database-specific keywords for MSSQL & Postgres
- Operators considered harmful (removed for functions `IN`, `NOT IN`, `LIKE` and `NOT LIKE`)
- Minimal supported version is `NET 6.0`

If you still need/want to use `v3.0`, follow the [Version 3 docs](README_v3.md).


## FAQ

### Why another library around Dapper?
I've created this library to cover most of my own use-cases where in 90% I need just a few simple queries for CRUD operations using Dapper and don't want to write column names manually. All I need is a simple record with properties and want to have them filled from the query or to insert / update data.

### How does the library works?
This library does two things:

1. Provides 4 computation expression builders for `select`, `insert`, `update` and `delete`. Those expressions create definitions (just simple records, no worries) of SQL queries.
2. Extends `IDbConnection` with few more methods to handle such definitions and creates proper SQL query + parameters for Dapper. Then it calls Dapper `QueryAsync` or `ExecuteAsync`. How does the library know the column names? It uses reflection to get record properties. So yes, there is one (the only) simple rule: *All property names must match columns in the table.*

### Do I need to create a record with all columns?
You can, but don't have to. If you need to read a subset of data only, you can create a special *view* record just for this. Also if you don't want to write nullable data, you can omit them in the record definition.

### And what about names mapping using Attributes or foreign keys magic?
Nope. Sorry. Not gonna happen in this library. Simplicity is what matters. Just define your record as it is in a database and you are ok.

### Can I map more records from one query?
Yes. If you use LEFT or INNER JOIN, you can map each table to a separate record. If you use LEFT JOIN, you can even map the 2nd and/or 3rd table to `Option` (F# records and `null` values don't work well together). The current limitation is 3 tables (two joins).

### What if I need to join more than 3 tables, sub-select or something special?
Fallback to plain Dapper then. Really. Dapper is an amazing library and sometimes there's nothing better than manually written optimized SQL query. Remember this library has one and only goal: Simplify 90% of repetitive SQL queries you would have to write manually. Nothing. Else.

## Getting started

First of all, you need to init registration of mappers for optional types to have Dapper mappings understand that `NULL` from database = `Option.None`

```f#
// for MSSQL
Dapper.FSharp.MSSQL.OptionTypes.register()

// for MySQL
Dapper.FSharp.MySQL.OptionTypes.register()

// for PostgreSQL
Dapper.FSharp.PostgreSQL.OptionTypes.register()

// for SQLite
Dapper.FSharp.SQLite.OptionTypes.register()
```

It's recommended to do it somewhere close to the program entry point or in `Startup` class.

### Example database

Let's have a database table called `Persons`:

```sql
CREATE TABLE [dbo].[Persons](
    [Id] [uniqueidentifier] NOT NULL,
    [FirstName] [nvarchar](max) NOT NULL,
    [LastName] [nvarchar](max) NOT NULL,
    [Position] [int] NOT NULL,
    [DateOfBirth] [datetime] NULL)
```

As mentioned in FAQ section, you need F# record to work with such table in `Dapper.FSharp`:

```f#
type Person = {
    Id : Guid
    FirstName : string
    LastName : string
    Position : int
    DateOfBirth : DateTime option
}
```

If you prefer not exposing your records, you can use internal types:

```f#
type internal Person = {
    Id : Guid
    FirstName : string
    LastName : string
    Position : int
    DateOfBirth : DateTime option
}
```

*Hint: Check tests located under tests/Dapper.FSharp.Tests folder for more examples*


## API Overview

### Table Mappings
You can either specify your tables within the query, or you can specify them above your queries (which is recommended since it makes them sharable between your queries).
The following will assume that the table name exactly matches the record name, "Person":

```F#
let personTable = table<Person>
```

If your record maps to a table with a different name:

```F#
let personTable = table'<Person> "People"
```

If you want to include a schema name:

```F#
let personTable = table'<Person> "People" |> inSchema "dbo"
```

### INSERT

Inserting a single record:

```f#
open Dapper.FSharp.MSSQL

let conn : IDbConnection = ... // get it somewhere

let newPerson = { Id = Guid.NewGuid(); FirstName = "Roman"; LastName = "Provaznik"; Position = 1; DateOfBirth = None }

let personTable = table<Person>

insert {
    into personTable
    value newPerson
} |> conn.InsertAsync
```

Inserting Multiple Records:

```f#
open Dapper.FSharp.MSSQL

let conn : IDbConnection = ... // get it somewhere

let person1 = { Id = Guid.NewGuid(); FirstName = "Roman"; LastName = "Provaznik"; Position = 1; DateOfBirth = None }
let person2 = { Id = Guid.NewGuid(); FirstName = "Ptero"; LastName = "Dactyl"; Position = 2; DateOfBirth = None }

let personTable = table<Person>

insert {
    into personTable
    values [ person1; person2 ]
} |> conn.InsertAsync
```

Excluding Fields from the Insert:

```f#
open Dapper.FSharp.MSSQL

let conn : IDbConnection = ... // get it somewhere

let newPerson = { Id = Guid.NewGuid(); FirstName = "Roman"; LastName = "Provaznik"; Position = 1; DateOfBirth = None }

let personTable = table<Person>

insert {
    for p in personTable do
    value newPerson
    excludeColumn p.DateOfBirth
} |> conn.InsertAsync
```

_NOTE: You can exclude multiple fields by using multiple `excludeColumn` statements._

### UPDATE

```F#
let updatedPerson = { existingPerson with LastName = "Vorezprut" }

update {
    for p in personTable do
    set updatedPerson
    where (p.Id = updatedPerson.Id)
} |> conn.UpdateAsync
```

Partial updates are possible by manually specifying one or more `includeColumn` properties:

```F#
update {
    for p in personTable do
    set modifiedPerson
    includeColumn p.FirstName
    includeColumn p.LastName
    where (p.Position = 1)
} |> conn.UpdateAsync
```


Partial updates are also possible by using `setColumn` keyword:

```F#
update {
    for p in personTable do
    setColumn p.FirstName "UPDATED"
    setColumn p.LastName "UPDATED"
    where (p.Position = 1)
} |> conn.UpdateAsync
```

### DELETE

```F#
delete {
    for p in personTable do
    where (p.Position = 10)
} |> conn.DeleteAsync
```

And if you really want to delete the whole table, you must use the `deleteAll` keyword:

```F#
delete {
    for p in personTable do
    deleteAll
} |> conn.DeleteAsync
```

### SELECT

To select all records in a table, you must use the `selectAll` keyword:

```F#
select {
    for p in personTable do
    selectAll
} |> conn.SelectAsync<Person>
```

NOTE: You also need to use `selectAll` if you have a no `where` and no `orderBy` clauses because a query cannot consist of only `for` or `join` statements.

NOTE: The type does not have to have all columns from the table. You can create a record with only the columns you need.

NOTE: This same approach will enable you to query views.

Filtering with where statement:

```F#
select {
    for p in personTable do
    where (p.Position > 5 && p.Position < 10)
} |> conn.SelectAsync<Person>
```

To flip boolean logic in `where` condition, use `not` operator (unary NOT):

```F#
select {
    for p in personTable do
    where (not (p.Position > 5 && p.Position < 10))
} |> conn.SelectAsync<Person>
```

You can also combine multiple `where` conditions with `andWhere` and `orWhere`:

```F#
select {
    for p in personTable do
    where (p.Position > 5)
    andWhere (p.Position < 10)
    orWhere (p.Position < 2)
} |> conn.SelectAsync<Person>
```

To conditionally add `where` part, you can use `andWhereIf` and `orWhereIf`:

```F#
let pos = Some 10
let posOr = Some 2
select {
    for p in personTable do
    where (p.Position > 5)
    andWhereIf pos.IsSome (p.Position < pos.Value)
    orWhereIf posOr.IsSome (p.Position < posOr.Value)
} |> conn.SelectAsync<Person>
```

NOTE: Do not use the forward pipe `|>` operator in your query expressions because it's not implemented, so don't do it (unless you like exceptions)!

To use LIKE operator in `where` condition, use `like`:
```F#
select {
    for p in personTable do
    where (like p.FirstName "%partofname%")
} |> conn.SelectAsync<Person>
```

To use IN operator in `where` condition, use `isIn`:
```F#
select {
    for p in personTable do
    where (isIn p.FirstName ["Elizabeth"; "Philipp"])
} |> conn.SelectAsync<Person>
```

You can also negate the IN operator in `where` condition, with `isNotIn`:
```F#
select {
    for p in personTable do
    where (isNotIn p.FirstName ["Charles"; "Camilla"])
} |> conn.SelectAsync<Person>
```

Sorting:

```F#
select {
    for p in personTable do
    where (p.Position > 5 && p.Position < 10)
    orderBy p.Position
    thenByDescending p.LastName
} |> conn.SelectAsync<Person>
```

If you need to skip some values or to take a subset of results only, use skip, take, and skipTake. Keep in mind that for correct paging, you need to order results as well.

```F#
select {
    for p in personTable do
    where (p.Position > 5 && p.Position < 10)
    orderBy p.Position
    skipTake 2 3 // skip first 2 rows, take next 3
} |> conn.SelectAsync<Person>
```

#### Option Types and Nulls

Checking for null on an Option type:
```F#
select {
    for p in personTable do
    where (p.DateOfBirth = None)
    orderBy p.Position
} |> conn.SelectAsync<Person>
```

Checking for null on a nullable type:
```F#
select {
    for p in personTable do
    where (p.LastName = null)
    orderBy p.Position
} |> conn.SelectAsync<Person>
```

Checking for null (works for any type):
```F#
select {
    for p in personTable do
    where (isNullValue p.LastName && isNotNullValue p.FirstName)
    orderBy p.Position
} |> conn.SelectAsync<Person>
```

Comparing an Option Type

```F#
let dob = DateTime.Today

select {
    for p in personTable do
    where (p.DateOfBirth = Some dob)
    orderBy p.Position
} |> conn.SelectAsync<Person>
```

### JOINS

For simple queries with join, you can use innerJoin and leftJoin in combination with SelectAsync overload:


```F#
let personTable = table<Person>
let dogsTable = table<Dog>
let dogsWeightsTable = table<DogsWeight>

select {
    for p in personTable do
    innerJoin d in dogsTable on (p.Id = d.OwnerId)
    orderBy p.Position
} |> conn.SelectAsync<Person, Dog>
```

`Dapper.FSharp` will map each joined table into a separate record and return it as list of `'a * 'b` tuples. Currently, up to 4 joins are supported, so you can also join another table here:

```F#
select {
    for p in personTable do
    innerJoin d in dogsTable on (p.Id = d.OwnerId)
    innerJoin dw in dogsWeightsTable on (d.Nickname = dw.DogNickname)
    orderBy p.Position
} |> conn.SelectAsync<Person, Dog, DogsWeight>
```

The problem with LEFT JOIN is that tables "on the right side" can be full of null values. Luckily we can use SelectAsyncOption to map joined values to Option types:

```F#
// this will return seq<(Person * Dog option * DogWeight option)>
select {
    for p in personTable do
    leftJoin d in dogsTable on (p.Id = d.OwnerId)
    leftJoin dw in dogsWeightsTable on (d.Nickname = dw.DogNickname)
    orderBy p.Position
} |> conn.SelectAsyncOption<Person, Dog, DogsWeight>
```

### Aggregate functions

Aggregate functions include `count`, `avg`, `sum`, `min`, and `max`. To fully support these functions in builder syntax, the `groupBy`, `groupByMany` and `distinct` keywords are supported as well.

See this example of how to get the amount of persons having a position value greater than 5:

```f#
select {
    for p in persons do
    count "*" "Value" // column name and alias (must match the view record property!!!)
    where (p.Position > 5)
} |> conn.SelectAsync<{| Value : int |}>
```

Or get the maximum value of the Position column from the table:

```f#
select {
    for p in persons do
    max "Position" "Value"
} |> conn.SelectAsync<{| Value : int |}>
```


Please keep in mind that work with aggregate functions can quickly turn into a nightmare. Use them wisely and if you'll find something hard to achieve using this library, better fallback to plain Dapper and good old handwritten queries™.


## OUTPUT clause support (MSSQL & PostgreSQL only)
This library supports `OUTPUT` clause for MSSQL & PostgreSQL using special methods: `InsertOutputAsync`, `UpdateOutputAsync` and `DeleteOutputAsync`. Please check tests located under tests/Dapper.FSharp.Tests folder for more examples.

## INSERT or REPLACE (SQLite only)
This library supports `INSERT or REPLACE` clause for SQLite using special method: `InsertOrReplaceAsync`.


## Deconstructor
To provide better usage with plain Dapper, this library contains `Deconstructor` converting `Dapper.FSharp` queries to a tuple of parameterized SQL query and `Map` of parameter values.

```f#
let r = {
    Id = Guid.NewGuid()
    FirstName = "Works"
    LastName = "Great"
    DateOfBirth = DateTime.Today
    Position = 1
}

let sql, values =
    insert {
        into personTable
        value r
    } |> Deconstructor.insert

printfn "%s" sql 
// INSERT INTO Persons (Id, FirstName, LastName, Position, DateOfBirth) 
// VALUES (@Id0, @FirstName0, @LastName0, @Position0, @DateOfBirth0)"

printfn "%A" values
// map [("DateOfBirth0", 11.05.2020 0:00:00); 
//      ("FirstName0", "Works");
//      ("Id0", 8cc6a7ed-7c17-4bea-a0ca-04a3985d2c7e); 
//      ("LastName0", "Great");
//      ("Position0", 1)]
```

## Database-specific syntax

Since version 4 `Dapper.FSharp` supports database-specific syntax.

### MSSQL

| Query  | Keyword                    | Description                                             |
|--------|----------------------------|---------------------------------------------------------|
| SELECT | `optionRecompile`          | Adds `OPTION(RECOMPILE)` as the query option            |
| SELECT | `optionOptimizeForUnknown` | Adds `OPTION(OPTIMIZE FOR UNKNOWN)` as the query option |

### PostgreSQL

| Query | Keyword    | Description                          |
|-------|------------|--------------------------------------|
| ALL   | `iLike`    | Adds `ILIKE` for WHERE condition     |
| ALL   | `notILike` | Adds `NOT ILIKE` for WHERE condition |


## IncludeColumn vs ExcludeColumn (there can be a 🐲)

New keywords added in `v2` - `excludeColumn` and `includeColumn` are a great addition to this library, especially when you want to do partial updates / inserts. However, be aware that you should **never mix both** in the same computation expression!

### ExcludeColumn
If used for the first time within computation expression all fields from the record will be used and removed (ignored) those you provided in a keyword. When used more times, already filtered fields will be filtered again.

### IncludeColumn
If used, only specified columns will be used and all the others will be ignored.

With great power comes great responsibility.

## Contribution Guide

Every new idea of how to make the library even better is more than welcome! However please be aware that there is a process we should all follow:

- [Create an issue](https://github.com/Dzoukr/Dapper.FSharp/issues/new) with a description of proposed changes
- Describe the expected impact on the library (API, performance, ...)
- Define if it's minor or breaking change
- Wait for Approve / Deny
- Send a PR (or wait until taken by some of the contributors)
