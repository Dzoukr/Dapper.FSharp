module Dapper.FSharp.Tests.MSSQL.InsertTests

open System.Data
open Expecto
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

    testTask "Inserts and outputs new record" {
        do! Persons.init conn
        let r = Persons.View.generate 1 |> List.head
        let! fromDb =
            insert {
                table "Persons"
                value r
            } |> conn.InsertOutputAsync<Persons.View, Persons.View>
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
]