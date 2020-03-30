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
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Position" 5)
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""            
    }
    
    testTask "Selects by single where condition with table name used" {
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
                where (eq "Persons.Position" 5)
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""            
    }
    
    testTask "Selects by IN where condition" {
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
                where (isIn "Position" [5;6])
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""            
        Expect.equal (rs |> List.find (fun x -> x.Position = 6)) (Seq.last fromDb) ""            
    }
    
    testTask "Selects by NOT IN where condition" {
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
                where (isNotIn "Position" [1;2;3])
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 4)) (Seq.head fromDb) ""            
        Expect.equal (rs |> List.find (fun x -> x.Position = 10)) (Seq.last fromDb) ""
        Expect.equal 7 (Seq.length fromDb) ""
    }
    
    testTask "Selects by IS NULL where condition" {
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
                where (isNullValue "DateOfBirth")
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 2)) (Seq.head fromDb) ""            
        Expect.equal 5 (Seq.length fromDb) ""
    }
    
    testTask "Selects by IS NOT NULL where condition" {
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
                where (isNotNullValue "DateOfBirth")
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 1)) (Seq.head fromDb) ""            
        Expect.equal 5 (Seq.length fromDb) ""
    }
    
    testTask "Selects by UNARY NOT where condition" {
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
                where !! (gt "Position" 5 + isNullValue "DateOfBirth")
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 9)) (Seq.last fromDb) ""            
        Expect.equal 7 (Seq.length fromDb) ""
    }
        
    testTask "Selects by multiple where conditions" {
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
                where (gt "Position" 2 + lt "Position" 4)
            } |> conn.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 3)) (Seq.head fromDb) ""            
    }
    
    testTask "Selects with order by" {
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
                orderBy "Position" Desc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal 10 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""            
    }
    
    testTask "Selects with skip parameter" {
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
                skip 5
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 5 (fromDb |> Seq.length) ""
    }
    
    testTask "Selects with skipTake parameter" {
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
                skipTake 5 2
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 2 (fromDb |> Seq.length) ""
    }
    
    testTask "Selects with one inner join - 1:1" {
        do! Persons.init conn
        do! Dogs.init conn
        
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1to1 persons
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
        let! fromDb =
            select {
                table "Persons"
                innerJoin "Dogs" "OwnerId" "Persons.Id"
            } |> conn.SelectAsync<Persons.View, Dogs.View>
        
        Expect.equal 10 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
    }
    
    testTask "Selects with one inner join - 1:N" {
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
        let! fromDb =
            select {
                table "Persons"
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
        let! fromDb =
            select {
                table "Persons"
                leftJoin "Dogs" "OwnerId" "Persons.Id"
                orderBy "Persons.Position" Asc
            } |> conn.SelectAsyncOption<Persons.View, Dogs.View>
        
        let byOwner = fromDb |> Seq.groupBy fst
        
        Expect.equal 14 (Seq.length fromDb) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
        Expect.isTrue (fromDb |> Seq.last |> snd |> Option.isNone) ""
        Expect.equal (dogs |> List.head |> Some) (fromDb |> Seq.head |> snd) ""
    }
    
    testTask "Selects with two inner joins - 1:1" {
        do! Persons.init conn
        do! Dogs.init conn
        do! DogsWeights.init conn
        
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1to1 persons
        let weights = DogsWeights.View.generate1to1 dogs
        
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
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> conn.InsertAsync
            
        let! fromDb =
            select {
                table "Persons"
                innerJoin "Dogs" "OwnerId" "Persons.Id"
                innerJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
                orderBy "Persons.Position" Asc
            } |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>
        
        Expect.equal 10 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head, weights.Head) (Seq.head fromDb) ""
    }
    
    testTask "Selects with two inner joins - 1:N" {
        do! Persons.init conn
        do! Dogs.init conn
        do! DogsWeights.init conn
        
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let weights = DogsWeights.View.generate1toN 3 dogs.Head
        
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
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> conn.InsertAsync
            
        let! fromDb =
            select {
                table "Persons"
                innerJoin "Dogs" "OwnerId" "Persons.Id"
                innerJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
                orderBy "Persons.Position" Asc
            } |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>
        
        Expect.equal 3 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head, weights.Head) (Seq.head fromDb) ""
    }
    
    testTask "Selects with two left joins" {
        do! Persons.init conn
        do! Dogs.init conn
        do! DogsWeights.init conn
        
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let weights = DogsWeights.View.generate1toN 3 dogs.Head
        
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
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> conn.InsertAsync
            
        let! fromDb =
            select {
                table "Persons"
                leftJoin "Dogs" "OwnerId" "Persons.Id"
                leftJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
                orderBy "Persons.Position" Asc
            } |> conn.SelectAsyncOption<Persons.View, Dogs.View, DogsWeights.View>
        
        let p1,d1,w1 = fromDb |> Seq.head
        Expect.equal persons.Head p1 ""
        Expect.equal (Some dogs.Head) d1 ""
        Expect.equal (Some weights.Head) w1 ""
        
        let pn,dn,wn = fromDb |> Seq.last
        Expect.equal (persons |> Seq.last) pn ""
        Expect.equal None dn ""
        Expect.equal None wn ""
        Expect.equal 16 (Seq.length fromDb) ""
    }
]