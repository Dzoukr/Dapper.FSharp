module Dapper.FSharp.Tests.MSSQL.DeleteTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL

let tests (conn:IDbConnection) = Tests.testList "DELETE" [
    
    testTask "Deletes single records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                table Persons.tableName
                where (column "Position" (Eq 10))
            } |> conn.DeleteAsync
        let! fromDb =
            select {
                table Persons.tableName
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
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                table Persons.tableName
                where (column "Position" (Ge 7))
            } |> conn.DeleteAsync
        
        let! fromDb =
            select {
                table Persons.tableName
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal 6 (Seq.length fromDb) ""
    }
]