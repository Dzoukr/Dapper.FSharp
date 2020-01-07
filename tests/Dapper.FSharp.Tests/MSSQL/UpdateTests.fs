module Dapper.FSharp.Tests.MSSQL.UpdateTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL

let tests (conn:IDbConnection) = Tests.testList "UPDATE" [
    
    testTask "Updates single records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table Persons.tableName
                set {| LastName = "UPDATED" |}
                where (column "Position" (Eq 2))
            } |> conn.UpdateAsync
        let! fromDb =
            select {
                table Persons.tableName
                where (column "LastName" (Eq "UPDATED"))
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
    
    testTask "Updates more records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table Persons.tableName
                set {| LastName = "UPDATED" |}
                where (column "Position" (Gt 7))
            } |> conn.UpdateAsync
        
        let! fromDb =
            select {
                table Persons.tableName
                where (column "LastName" (Eq "UPDATED"))
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal 3 (Seq.length fromDb) ""
    }
]