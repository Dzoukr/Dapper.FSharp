module Dapper.FSharp.Tests.MySQL.DeleteTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.MySQL.Database
open Dapper.FSharp
open Dapper.FSharp.MySQL

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
]