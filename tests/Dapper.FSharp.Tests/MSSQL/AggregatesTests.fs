module Dapper.FSharp.Tests.MSSQL.AggregatesTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL

let tests (conn:IDbConnection) = Tests.testList "SELECT - AGGREGATES" [
    testTask "Selects with COUNT aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                count "*" "Value"
            }
            |> conn.SelectAsync<{| Value : int |}>
            |> taskToList
        Expect.equal 10 fromDb.Head.Value ""
    }
    
    testTask "Selects with COUNT aggregate function + column" {
        do! Persons.init conn
        let rs =
            Persons.View.generate 10
            |> List.map (fun x -> if x.Position > 5 then { x with Position = 10 } else x)
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                count "*" "Value"
                groupBy "Position"
            }
            |> conn.SelectAsync<{| Value : int; Position : int |}>
            |> taskToList
            |> List.rev
        Expect.equal 6 fromDb.Length ""
        Expect.equal 10 fromDb.Head.Position ""
        Expect.equal 5 fromDb.Head.Value ""
    }
    
    testTask "Selects with COUNT aggregate function + WHERE" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                count "*" "Value"
                where (gt "Position" 5)
            }
            |> conn.SelectAsync<{| Value : int |}>
            |> taskToList
        Expect.equal 5 fromDb.Head.Value ""
    }
    
    testTask "Selects with AVG aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                avg "Position" "Value"
            }
            |> conn.SelectAsync<{| Value : int |}>
            |> taskToList
        Expect.equal 5 fromDb.Head.Value ""
    }
    
    testTask "Selects with SUM aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                sum "Position" "Value"
            }
            |> conn.SelectAsync<{| Value : int |}>
            |> taskToList
        Expect.equal 55 fromDb.Head.Value ""
    }
    
    testTask "Selects with MIN aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                min "Position" "Value"
            }
            |> conn.SelectAsync<{| Value : int |}>
            |> taskToList
        Expect.equal 1 fromDb.Head.Value ""
    }
    
    testTask "Selects with MAX aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                max "Position" "Value"
            }
            |> conn.SelectAsync<{| Value : int |}>
            |> taskToList
        Expect.equal 10 fromDb.Head.Value ""
    }
    
    testTask "Select distinct" {
        do! Persons.init conn
        do! Dogs.init conn

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                table "Persons"
                values persons
            } |> conn.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> conn.InsertAsync

        let fromDb =
            select {
                table "Persons"
                distinct
                leftJoin "Dogs" "OwnerId" "Persons.Id"
            }
            |> conn.SelectAsync<{| FirstName:string |}>
            |> taskToList

        Expect.equal 10 fromDb.Length ""
    }
    
    testTask "Selects with multiple aggregate functions" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                table "Persons"
                max "Position" "MaxValue"
                min "Position" "MinValue"
            }
            |> conn.SelectAsync<{| MaxValue : int; MinValue : int |}>
            |> taskToList

        Expect.equal 10 fromDb.Head.MaxValue ""
        Expect.equal 1 fromDb.Head.MinValue ""
    }
    
    testTask "Select group by aggregate" {
        do! Persons.init conn
        do! Dogs.init conn

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                table "Persons"
                values persons
            } |> conn.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> conn.InsertAsync

        let one,two =
            select {
                table "Persons"
                leftJoin "Dogs" "OwnerId" "Persons.Id"
                count "Persons.Position" "Count"
                groupBy ["Persons.Id"; "Persons.Position"; "Dogs.OwnerId"]
                orderBy "Persons.Position" Asc
            }
            |> conn.SelectAsync<{| Id: System.Guid; Position:int; Count:int |}, {| OwnerId : System.Guid |}>
            |> taskToList
            |> List.head
            
        Expect.equal 5 one.Count ""
        Expect.equal 1 one.Position ""
        Expect.equal one.Id two.OwnerId ""
    } 
]