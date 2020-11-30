module Dapper.FSharp.Tests.DeleteTests

open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "DELETE" [
    
    testTask "Deletes single records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! _ =
            delete {
                table "Persons"
                where (eq "Position" 10)
            } |> crud.DeleteAsync
        let! fromDb =
            select {
                table "Persons"
                orderBy "Position" Desc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 9 (Seq.length fromDb) ""
        Expect.equal 9 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Deletes more records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! _ =
            delete {
                table "Persons"
                where (ge "Position" 7)
            } |> crud.DeleteAsync

        let! fromDb =
            select {
                table "Persons"
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (Seq.length fromDb) ""
    }
]

let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "DELETE OUTPUT" [
    
    testTask "Deletes and outputs single record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            delete {
                table "Persons"
                where (eq "Position" 10)
            } |> crud.DeleteOutputAsync<Persons.View>
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 10 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Deletes and outputs multiple records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! insertedPersonIds =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertOutputAsync
        let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:System.Guid |}) -> p.Id) |> Seq.toList
        let boxedPersonIds = personIds |> List.map box |> Seq.toList

        let! deleted =
            delete {
                table "Persons"
                where (isIn "Id" boxedPersonIds)
            } |> crud.DeleteOutputAsync<Persons.View>
        Expect.hasLength deleted 10 ""
        deleted |> Seq.iter (fun (p:Persons.View) ->
            Expect.isTrue (personIds |> List.exists ((=) p.Id)) "Deleted personId not found from inserted Ids")
    }
]