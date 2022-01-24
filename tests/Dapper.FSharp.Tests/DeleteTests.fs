module Dapper.FSharp.Tests.DeleteTests

open Dapper.FSharp
open Dapper.FSharp.Builders
open Dapper.FSharp.Tests.Database
open Expecto
open System.Threading
open Dapper.FSharp.Tests.Extensions
open System.Threading.Tasks

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "DELETE" [

    let personsView = table'<Persons.View> "Persons"
    
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

    testTask "Cancellation" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        use cts = new CancellationTokenSource()
        cts.Cancel()
        let deleteCrud query =
            crud.DeleteAsync(query, cancellationToken = cts.Token) :> Task
        let action () = 
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> deleteCrud 
        do! Expect.throwsTaskCanceledException action "Should be canceled action"
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

let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "DELETE OUTPUT" [
    
    let personsView = table'<Persons.View> "Persons"


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