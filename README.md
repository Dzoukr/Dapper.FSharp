# Dapper.FSharp [![NuGet](https://img.shields.io/nuget/v/Dapper.FSharp.svg?style=flat)](https://www.nuget.org/packages/Dapper.FSharp/)

<p align="center">
<img src="https://github.com/Dzoukr/Dapper.FSharp/raw/master/logo.png" width="150px"/>
</p>

Lightweight F# extension for StackOverflow Dapper

## Features

- No *auto-attribute-based-only-author-maybe-knows-magic* behavior
- Support for F# records / anonymous records
- Support for F# options
- Support for SQL Server 2012 (11.x) and later / Azure SQL Database
- Support for SELECT (including JOINs), INSERT, UPDATE (full / partial), DELETE
- Support for OUTPUT clause
- Easy usage thanks to F# computation expressions
- Keeps things simple

## Installation
If you want to install this package manually, use usual NuGet package command 

    Install-Package Dapper.FSharp

or using [Paket](http://fsprojects.github.io/Paket/getting-started.html) 

    paket add Dapper.FSharp

## FAQ

### Why another library around Dapper?
I've created this library to cover most of my own use-cases where in 90% I need just few simple queries for CRUD operations using Dapper and don't want to write column names manually. All I need is simple (anonymous) record with properties and want to have them filled from query or to insert / update data.

### How does library works?
This library does two things:

1. Provides 4 computation expression builders for `select`, `insert`, `update` and `delete`. Those expressions creates definitions (just simple records, no worries) of SQL queries.
2. Extends `IDbConnection` with few more methods to handle such definitions and creates proper SQL query + parameters for Dapper. Then it calls Dapper `QueryAsync` or `ExecuteAsync`. How does library knows the column names? It uses reflection to get record properties. So yes, there is one (the only) simple rule: *All property names must match columns in table.*

### Do I need to create record with all columns?
You can, but don't have to. If you need to read only subset of data, you can create special *view* record just for this. Also if you don't want to write nullable data, you can omit them in record definition. 

### And what about names mapping using Attributes or foreign keys magic?
Nope. Sorry. Not gonna happen in this library. Simplicity is what matters. Just define your record as it is in database and you are ok.

### Can I map more records from one query?
Yes. If you use LEFT or INNER JOIN, you can map each table to separate record. If you use LEFT JOIN, you can even map 2nd and/or 3rd table to `Option` (F# records and `null` values don't work well together). Current limitation is 3 tables (two joins).

### What if I need join more than 3 tables, sub-select or something special?
Fallback to plain Dapper then. Really. Dapper is amazing library and sometimes there's nothing better than manually written optimized SQL query. Remember this library has one and only goal: Simplify 90% of repetitive SQL queries you would have to write manually. Nothing. Else.

## Getting started

First of all, you need to init registration of mappers for optional types to have Dapper mappings understand that `NULL` from database = `Option.None`

```f#
Dapper.FSharp.OptionTypes.register()
```

It's recommended to do it somewhere close to program entry point or in `Startup` class.

### Example database

Lets have a database table called `Persons`:

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

*Hint: Check tests located under tests/Dapper.FSharp.Tests folder for more examples*

### INSERT

To insert new values into `Persons` table, use `insert` computation expression:

```f#
open Dapper.FSharp
open Dapper.FSharp.MSSQL

let conn : IDbConnection = ... // get it somewhere

let newPerson = { Id = Guid.NewGuid(); FirstName = "Roman"; LastName = "Provaznik"; Position = 1; DateOfBirth = None }

insert {
    table "Persons"
    value newPerson
} |> conn.InsertAsync
```

If you have more `Persons` to insert, use `values` instead of `value`.

```f#
let newPerson1 = { Id = Guid.NewGuid(); FirstName = "Roman"; LastName = "Provaznik"; Position = 1; DateOfBirth = None }
let newPerson2 = { Id = Guid.NewGuid(); FirstName = "Jiri"; LastName = "Landsman"; Position = 2; DateOfBirth = None }

insert {
    table "Persons"
    values [newPerson1; newPerson2]
} |> conn.InsertAsync
```

You can insert only part of data, but keep in mind that you need to write all necessary columns or you'll get an error on SQL level:

```f#
insert {
    table "Persons"
    value {| Id = Guid.NewGuid(); FirstName = "Without"; LastName = "Birth date"; Position = 3 |}
} |> conn.InsertAsync
```

*Note: All methods are asynchronous (returning Task) so you must "bang" (await) them. This part is skipped in examples.*

### WHERE condition

Since version `1.1` there are few helper functions available to make syntax shorter.

Longer syntax:

```f#
where (column "Id" (Eq updatedPerson.Id))
```

Shorter syntax:

```f#
where (eq "Id" updatedPerson.Id)
```

*Note: The longer syntax is still valid and it's up to your personal taste which one you gonna use.*


### UPDATE

As you can insert values, you can update them:

```f#
let updatedPerson = { existingPerson with LastName = "Vorezprut" }
update {
    table "Persons"
    set updatedPerson
    where (eq "Id" updatedPerson.Id)
} |> conn.UpdateAsync
```

Partial updates are also possible:

```f#
update {
    table "Persons"
    set {| LastName = "UPDATED" |}
    where (eq "Position" 1)
} |> conn.UpdateAsync
```

### DELETE

The same goes for delete, but please, for the mother of all backups, **don't forget where condition**:

```f#
delete {
    table "Persons"
    where (eq "Position" 10)
} |> conn.DeleteAsync
```

Did I say you should **never forget where condition** in delete?

### SELECT

Use `select` to read all values back from database. Please note that you need to set desired mapping type in generic `SelectAsync` method:

```f#
select {
    table "Persons"
} |> conn.SelectAsync<Person>
```

To filter values, use `where` condition as you know it from `update` and `delete`. Where conditions can be also combined with `(+) operator` (logical AND) or `(*) operator` (logical OR):

```f#
select {
    table "Persons"
    where (gt "Position" 5 + lt "Position" 10)
} |> conn.SelectAsync<Person>
```

To flip boolean logic in `where` condition, use `(!!) operator` (unary NOT):

```f#
select {
    table "Persons"
    where !! (gt "Position" 5 + lt "Position" 10)
} |> conn.SelectAsync<Person>
```

Sorting works as you would expect:

```f#
select {
    table "Persons"
    where (gt "Position" 5 + lt "Position" 10)
    orderBy "Position" Asc
} |> conn.SelectAsync<Person>
```

If you need to skip some values or take only subset of results, use `skip` and `skipTake`. Keep in mind that for correct paging, you need to order results as well.

```f#
select {
    table "Persons"
    where (gt "Position" 5 + lt "Position" 10)
    orderBy "Position" Asc
    skipTake 2 3 // skip first 2 rows, take next 3
} |> conn.SelectAsync<Person>
```

### SELECT WITH JOIN

For simple queries with join, you can use `innerJoin` and `leftJoin` in combination with `SelectAsync` overload:

```f#
select {
    table "Persons"
    innerJoin "Dogs" "OwnerId" "Persons.Id"
    orderBy "Persons.Position" Asc
} |> conn.SelectAsync<Person, Dog>
```

`Dapper.FSharp` will map each joined table into separate record and return it as list of `'a * 'b` tuples. Currently up to 2 joins are supported, so you can also join another table here:

```f#
select {
    table "Persons"
    innerJoin "Dogs" "OwnerId" "Persons.Id"
    innerJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
    orderBy "Persons.Position" Asc
} |> conn.SelectAsync<Person, Dog, DogsWeight>
```

Problem with `LEFT JOIN` is that tables "on the right side" can be full of null values. Luckily we can use `SelectAsyncOption` to map joined values to `Option` types:

```f#
// this will return seq<(Person * Dog option * DogWeight option)>
select {
    table "Persons"
    leftJoin "Dogs" "OwnerId" "Persons.Id"
    leftJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
    orderBy "Persons.Position" Asc
} |> conn.SelectAsyncOption<Person, Dog, DogsWeight>
``` 

## OUTPUT clause support
From version `1.4.0` this library supports `OUTPUT` clause using special methods: `InsertOutputAsync`, `UpdateOutputAsync` and `DeleteOutputAsync`. Please check tests located under tests/Dapper.FSharp.Tests folder for more examples.