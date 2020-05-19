module Dapper.FSharp.Tests.MSSQL.DeleteTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL

let tests (conn:IDbConnection) = Tests.testList "DELETE" [

    testTask "Deletes single records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                table "Persons"
                where (eq "Position" 10)
            } |> conn.DeleteAsync
        let! fromDb =
            select {
                table "Persons"
                orderBy "Position" Desc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal 9 (Seq.length fromDb) ""
        Expect.equal 9 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Deletes more records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                table "Persons"
                where (ge "Position" 7)
            } |> conn.DeleteAsync

        let! fromDb =
            select {
                table "Persons"
            } |> conn.SelectAsync<Persons.View>
        Expect.equal 6 (Seq.length fromDb) ""
    }


    /// OUTPUT tests

    testTask "Deletes and outputs single record" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            delete {
                table "Persons"
                where (eq "Position" 10)
            } |> conn.DeleteOutputAsync<Persons.View>
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 10 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Deletes and outputs multiple records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! insertedPersonIds =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertOutputAsync
        let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:System.Guid |}) -> p.Id) |> Seq.toList
        let boxedPersonIds = personIds |> List.map box |> Seq.toList

        let! deleted =
            delete {
                table "Persons"
                where (isIn "Id" boxedPersonIds)
            } |> conn.DeleteOutputAsync<Persons.View>
        Expect.hasLength deleted 10 ""
        deleted |> Seq.iter (fun (p:Persons.View) ->
            Expect.isTrue (personIds |> List.exists ((=) p.Id)) "Deleted personId not found from inserted Ids")
    }
]