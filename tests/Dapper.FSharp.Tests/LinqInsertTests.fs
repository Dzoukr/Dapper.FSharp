module Dapper.FSharp.Tests.LinqInsertTests

open Dapper.FSharp
open Dapper.FSharp.LinqBuilders
open Dapper.FSharp.Tests.Database
open Expecto

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "LINQ INSERT" [
    
    let personsView = entity<Persons.View> |> mapTable "Persons"

    testTask "Inserts new record" {
        do! init.InitPersons()
        let r = Persons.View.generate 1 |> List.head
        let! _ =
            insert {
                into personsView
                value r
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Id = r.Id)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal r (Seq.head fromDb) ""
    }

    // Not supported in Linq insert.
    // May possibly support as "partialValue" and "partialValues".
    //testTask "Inserts partial record" {
    //    do! init.InitPersons()
    //    let r =
    //        Persons.View.generate 1
    //        |> List.head
    //        |> fun x -> ({ Id = x.Id; FirstName = x.FirstName; LastName = x.LastName; Position = x.Position } : Persons.ViewRequired)
    //    let! _ =
    //        insert {
    //            into personsView
    //            value r
    //        } |> crud.InsertAsync
    //    let! fromDb =
    //        select {
    //            for p in personsView do
    //            where (p.Id = r.Id)
    //        } |> crud.SelectAsync<Persons.ViewRequired>
    //    Expect.equal r (Seq.head fromDb) ""
    //}

    testTask "Inserts more records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal rs (Seq.toList fromDb) ""
    }
]

let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "LINQ INSERT OUTPUT" [
    
    let personsView = entity<Persons.View> |> mapTable "Persons"

    testTask "Inserts and outputs single record" {
        do! init.InitPersons()
        let r = Persons.View.generate 1 |> List.head
        let! fromDb =
            insert {
                into personsView
                value r
            } |> crud.InsertOutputAsync<Persons.View, Persons.View> // Optional type specification
        Expect.equal r (Seq.head fromDb) ""
    }

    testTask "Inserts and outputs multiple records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! insertedPersons =
            insert {
                into personsView
                values rs
            } |> crud.InsertOutputAsync
        let generatedPositions = rs |> List.map (fun p -> p.Position)
        Expect.hasLength insertedPersons 10 ""
        insertedPersons |> Seq.iter (fun (p:Persons.View) ->
            Expect.isTrue (generatedPositions |> List.exists ((=) p.Position)) "Insert output person position not found from generated positions")
    }

    testTask "Inserts and outputs subset of single record columns" {
        do! init.InitPersons()
        let r = Persons.View.generate 1 |> List.head
        let! fromDb =
            insert {
                into personsView
                value r
            } |> crud.InsertOutputAsync
        Expect.equal r.Position (Seq.head fromDb |> fun (p:{| Position:int |}) -> p.Position) ""
    }

    testTask "Inserts row with None value and outputs record" {
        do! init.InitPersons()
        let r = Persons.View.generate 1 |> List.head |> fun p -> { p with DateOfBirth = None }
        let! fromDb =
            insert {
                into personsView
                value r
            } |> crud.InsertOutputAsync
        Expect.equal r (Seq.head fromDb) ""
    }

    testTask "Inserts row with Some value and outputs record" {
        do! init.InitPersons()
        let r = Persons.View.generate 1 |> List.head |> fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow }
        let! fromDb =
            insert {
                into personsView
                value r
            } |> crud.InsertOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal r.Id (Seq.head fromDb |> fun (p:Persons.View) -> p.Id) "" // Comparing Some <datetime> fails even though it is the same
    }
]