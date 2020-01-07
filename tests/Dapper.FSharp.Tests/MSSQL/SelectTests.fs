module Dapper.FSharp.Tests.MSSQL.SelectTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL
       
let tests (conn:IDbConnection) = Tests.testList "SELECT" [
    
    testTask "Selects by single where condition" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table Persons.tableName
                where (column "Position" (Eq 5))
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""            
    }
    
    testTask "Selects by multiple where conditions" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table Persons.tableName
                where (column "Position" (Gt 2) + column "Position" (Lt 4))
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 3)) (Seq.head fromDb) ""            
    }
    
    testTask "Selects with one inner join - 1:1" {
        do! Persons.init conn
        let persons = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values persons
            } |> conn.InsertAsync
        do! Dogs.init conn
        let dogs = Dogs.View.generate1to1 persons
        let! _ =
            insert {
                table Dogs.tableName
                values dogs
            } |> conn.InsertAsync
                    
        let! fromDb =
            select {
                table Persons.tableName
                innerJoin "Dogs" "OwnerId" "Persons.Id"
            } |> conn.SelectAsync<Persons.View, Dogs.View>
        
        Expect.equal 10 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
    }
    
    testTask "Selects with one inner join - 1:N" {
        do! Persons.init conn
        let persons = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values persons
            } |> conn.InsertAsync
        do! Dogs.init conn
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                table Dogs.tableName
                values dogs
            } |> conn.InsertAsync
                    
        let! fromDb =
            select {
                table Persons.tableName
                innerJoin "Dogs" "OwnerId" "Persons.Id"
            } |> conn.SelectAsync<Persons.View, Dogs.View>
        
        let byOwner = fromDb |> Seq.groupBy fst
        
        Expect.equal 5 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
        Expect.equal 1 (Seq.length byOwner) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
    }
    
    testTask "Selects with one left join" {
        do! Persons.init conn
        let persons = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values persons
            } |> conn.InsertAsync
        do! Dogs.init conn
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                table Dogs.tableName
                values dogs
            } |> conn.InsertAsync
                    
        let! fromDb =
            select {
                table Persons.tableName
                leftJoin "Dogs" "OwnerId" "Persons.Id"
                orderBy "Persons.Position" Asc
            } |> conn.SelectAsyncOption<Persons.View, Dogs.View>
        
        let byOwner = fromDb |> Seq.groupBy fst
        
        Expect.equal 14 (Seq.length fromDb) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
        Expect.isTrue (fromDb |> Seq.rev |> Seq.head |> snd |> Option.isNone) ""
        Expect.equal (dogs |> List.head |> Some) (fromDb |> Seq.head |> snd) ""
    }
]