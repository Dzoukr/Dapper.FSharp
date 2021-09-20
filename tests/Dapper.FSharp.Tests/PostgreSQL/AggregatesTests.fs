module Dapper.FSharp.Tests.PostgreSQL.AggregatesTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.PostgreSQL.Database
open Dapper.FSharp
open Dapper.FSharp.PostgreSQL

let persons = table'<Persons.View> "Persons"
let dogs = table'<Dogs.View> "Dogs"

let tests (conn:IDbConnection) = Tests.testList "SELECT - AGGREGATES" [
    testTask "Selects with COUNT aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
                count "*" "Value"
            }
            |> conn.SelectAsync<{| Value : int64 |}>
            |> taskToList
        Expect.equal 10L fromDb.Head.Value ""
    }
    
    testTask "Selects with COUNT aggregate function + column" {
        do! Persons.init conn
        let rs =
            Persons.View.generate 10
            |> List.map (fun x -> if x.Position > 5 then { x with Position = 10 } else x)
        let! _ =
            insert {
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
                count "*" "Value"
                orderByDescending p.Position
                groupBy p.Position
            }
            |> conn.SelectAsync<{| Value : int64; Position : int |}>
            |> taskToList
        
        Expect.equal 6 fromDb.Length ""
        Expect.equal 10 fromDb.Head.Position ""
        Expect.equal 5L fromDb.Head.Value ""
    }
    
    testTask "Selects with COUNT aggregate function + WHERE" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
                count "*" "Value"
                where (p.Position > 5)
            }
            |> conn.SelectAsync<{| Value : int64 |}>
            |> taskToList
        Expect.equal 5L fromDb.Head.Value ""
    }
    
    testTask "Selects with AVG aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
                avg "Position" "Value"
            }
            |> conn.SelectAsync<{| Value : decimal |}>
            |> taskToList
        Expect.equal 5.5M fromDb.Head.Value ""
    }
    
    testTask "Selects with SUM aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
                sum "Position" "Value"
            }
            |> conn.SelectAsync<{| Value : int64 |}>
            |> taskToList
        Expect.equal 55L fromDb.Head.Value ""
    }
    
    testTask "Selects with MIN aggregate function" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
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
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
                max "Position" "Value"
            }
            |> conn.SelectAsync<{| Value : int |}>
            |> taskToList
        Expect.equal 10 fromDb.Head.Value ""
    }
    
    testTask "Select distinct" {
        do! Persons.init conn
        do! Dogs.init conn

        let ps = Persons.View.generate 10
        let ds = Dogs.View.generate1toN 5 ps.Head
        let! _ =
            insert {
                into persons
                values ps
            } |> conn.InsertAsync
        let! _ =
            insert {
                into dogs
                values ds
            } |> conn.InsertAsync

        let fromDb =
            select {
                for p in persons do
                leftJoin d in dogs on (p.Id = d.OwnerId)
                distinct
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
                into persons
                values rs
            } |> conn.InsertAsync
        let fromDb =
            select {
                for p in persons do
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

        let ps = Persons.View.generate 10
        let ds = Dogs.View.generate1toN 5 ps.Head
        let! _ =
            insert {
                into persons
                values ps
            } |> conn.InsertAsync
        let! _ =
            insert {
                into dogs
                values ds
            } |> conn.InsertAsync

        let one,two =
            select {
                for p in persons do
                leftJoin d in dogs on (p.Id = d.OwnerId)
                count "Persons.Position" "Count"
                groupBy (p.Id, p.Position, d.OwnerId)
                orderBy p.Position
            }
            |> conn.SelectAsync<{| Id: System.Guid; Position:int; Count:int64 |}, {| OwnerId : System.Guid |}>
            |> taskToList
            |> List.head
            
        Expect.equal 5L one.Count ""
        Expect.equal 1 one.Position ""
        Expect.equal one.Id two.OwnerId ""
    } 
]