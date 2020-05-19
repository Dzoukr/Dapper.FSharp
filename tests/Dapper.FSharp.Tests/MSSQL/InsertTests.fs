module Dapper.FSharp.Tests.MSSQL.InsertTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL

let tests (conn:IDbConnection) = Tests.testList "INSERT" [

    testTask "Inserts new record" {
        do! Persons.init conn
        let r = Persons.View.generate 1 |> List.head
        let! _ =
            insert {
                table "Persons"
                value r
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Id" r.Id)
            } |> conn.SelectAsync<Persons.View>
        Expect.equal r (Seq.head fromDb) ""
    }

    testTask "Inserts partial record" {
        do! Persons.init conn
        let r =
            Persons.View.generate 1
            |> List.head
            |> fun x -> ({ Id = x.Id; FirstName = x.FirstName; LastName = x.LastName; Position = x.Position } : Persons.ViewRequired)
        let! _ =
            insert {
                table "Persons"
                value r
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Id" r.Id)
            } |> conn.SelectAsync<Persons.ViewRequired>
        Expect.equal r (Seq.head fromDb) ""
    }

    testTask "Inserts more records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal rs (Seq.toList fromDb) ""
    }

    /// OUTPUT tests

    testTask "Inserts and outputs single record" {
        do! Persons.init conn
        let r = Persons.View.generate 1 |> List.head
        let! fromDb =
            insert {
                table "Persons"
                value r
            } |> conn.InsertOutputAsync<Persons.View, Persons.View> // Optional type specification
        Expect.equal r (Seq.head fromDb) ""
    }

    testTask "Inserts and outputs multiple records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! insertedPersons =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertOutputAsync
        let generatedPositions = rs |> List.map (fun p -> p.Position)
        Expect.hasLength insertedPersons 10 ""
        insertedPersons |> Seq.iter (fun (p:Persons.View) ->
            Expect.isTrue (generatedPositions |> List.exists ((=) p.Position)) "Insert output person position not found from generated positions")
    }

    testTask "Inserts and outputs subset of single record columns" {
        do! Persons.init conn
        let r = Persons.View.generate 1 |> List.head
        let! fromDb =
            insert {
                table "Persons"
                value r
            } |> conn.InsertOutputAsync
        Expect.equal r.Position (Seq.head fromDb |> fun (p:{| Position:int |}) -> p.Position) ""
    }

    testTask "Inserts row with None value and outputs record" {
        do! Persons.init conn
        let r = Persons.View.generate 1 |> List.head |> fun p -> { p with DateOfBirth = None }
        let! fromDb =
            insert {
                table "Persons"
                value r
            } |> conn.InsertOutputAsync
        Expect.equal r (Seq.head fromDb) ""
    }

    testTask "Inserts row with Some value and outputs record" {
        do! Persons.init conn
        let r = Persons.View.generate 1 |> List.head |> fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow }
        let! fromDb =
            insert {
                table "Persons"
                value r
            } |> conn.InsertOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal r.Id (Seq.head fromDb |> fun (p:Persons.View) -> p.Id) "" // Comparing Some <datetime> fails even though it is the same
    }
]