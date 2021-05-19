module Dapper.FSharp.Tests.LinqDeleteTests

open Dapper.FSharp
open Dapper.FSharp.LinqBuilders
open Dapper.FSharp.Tests.Database
open Expecto

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "LINQ DELETE" [

    let personsView = entity<Persons.View> |> mapTable "Persons"
    
    testTask "Deletes single records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! _ =
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> crud.DeleteAsync
        let! fromDb =
            select {
                for p in personsView do
                orderByDescending p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 9 (Seq.length fromDb) ""
        Expect.equal 9 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Deletes more records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! _ =
            delete {
                for p in personsView do
                where (p.Position >= 7)
            } |> crud.DeleteAsync

        let! fromDb =
            select {
                for p in personsView do
                selectAll
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (Seq.length fromDb) ""
    }
]

let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "LINQ DELETE OUTPUT" [
    
    let personsView = entity<Persons.View> |> mapTable "Persons"

    testTask "Deletes and outputs single record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> crud.DeleteOutputAsync<Persons.View>
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 10 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Deletes and outputs multiple records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! insertedPersonIds =
            insert {
                into personsView
                values rs
            } |> crud.InsertOutputAsync
        let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:System.Guid |}) -> p.Id) |> Seq.toList

        let! deleted =
            delete {
                for p in personsView do
                where (isIn p.Id personIds)
            } |> crud.DeleteOutputAsync<Persons.View>
        Expect.hasLength deleted 10 ""
        deleted |> Seq.iter (fun (p:Persons.View) ->
            Expect.isTrue (personIds |> List.exists ((=) p.Id)) "Deleted personId not found from inserted Ids")
    }
]