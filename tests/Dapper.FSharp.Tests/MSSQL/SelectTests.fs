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

    testTask "Selects by LIKE where condition return matching rows" {
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
                where (like "FirstName" "First%")
            } |> conn.SelectAsync<Persons.View>
        Expect.isNonEmpty fromDb ""
        Expect.isTrue (fromDb |> Seq.forall (fun (p:Persons.View) -> p.FirstName.StartsWith "First")) ""
    }

    testTask "Selects by LIKE where condition do not return non-matching rows" {
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
                where (like "FirstName" "NonExistingName%")
            } |> conn.SelectAsync<Persons.View>
        Expect.isEmpty fromDb ""
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


    testTask "Select aggregate count on empty table" {
        do! Persons.init conn
        let! fromDb =
            select {
                table "Persons"
            } |> fun query -> conn.SelectAsync<{| count:int |}>(query, aggr = [ count "*" "count" ])
        Expect.equal 0 (fromDb |> Seq.head |> fun (x:{| count:int |}) -> x.count) ""
    }

    testTask "Select aggregate count" {
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
            } |> fun query -> conn.SelectAsync<{| count:int |}>(query, aggr = [ count "*" "count" ])
        Expect.equal 10 (fromDb |> Seq.head |> fun (x:{| count:int |}) -> x.count) ""
    }

    testTask "Select aggregate count with column name" {
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
            } |> fun query -> conn.SelectAsync<{| count:int |}>(query, aggr = [ count "Position" "count" ])
        Expect.equal 10 (fromDb |> Seq.head |> fun (x:{| count:int |}) -> x.count) ""
    }

    testTask "Select aggregate count with where condition" {
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
                where (eq "Position" 1)
            } |> fun query -> conn.SelectAsync<{| count:int |}>(query, aggr = [ count "*" "count" ])
        Expect.equal 1 (fromDb |> Seq.head |> fun (x:{| count:int |}) -> x.count) ""
    }

    testTask "Select aggregate avg with empty table" {
        do! Persons.init conn
        let! fromDb =
            select {
                table "Persons"
            } |> fun query -> conn.SelectAsync<{| avg:int option |}>(query, aggr = [ avg "Position" "avg" ])
        let result = fromDb |> Seq.head |> fun (x:{| avg:int option |}) -> x.avg
        Expect.isNone result ""
    }

    testTask "Select aggregate avg" {
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
            } |> fun query -> conn.SelectAsync<{| avg:int option |}>(query, aggr = [ avg "Position" "avg" ])
        let result = fromDb |> Seq.head |> fun (x:{| avg:int option |}) -> x.avg
        Expect.isSome result ""
        Expect.equal 5 result.Value ""
    }

    testTask "Select aggregate avg with where condition" {
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
                where (eq "Position" 1)
            } |> fun query -> conn.SelectAsync<{| avg:int option |}>(query, aggr = [ avg "Position" "avg" ])
        let result = fromDb |> Seq.head |> fun (x:{| avg:int option |}) -> x.avg
        Expect.isSome result ""
        Expect.equal 1 result.Value ""
    }

    testTask "Select aggregate sum with empty table" {
        do! Persons.init conn
        let! fromDb =
            select {
                table "Persons"
            } |> fun query -> conn.SelectAsync<{| sum:int option |}>(query, aggr = [ sum "Position" "sum" ])
        let result = fromDb |> Seq.head |> fun (x:{| sum:int option |}) -> x.sum
        Expect.isNone result ""
    }

    testTask "Select aggregate sum" {
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
            } |> fun query -> conn.SelectAsync<{| sum:int option |}>(query, aggr = [ sum "Position" "sum" ])
        let result = fromDb |> Seq.head |> fun (x:{| sum:int option |}) -> x.sum
        Expect.isSome result ""
        Expect.equal 55 result.Value ""
    }

    testTask "Select aggregate sum with where condition" {
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
                where (eq "Position" 1)
            } |> fun query -> conn.SelectAsync<{| sum:int option |}>(query, aggr = [ sum "Position" "sum" ])
        let result = fromDb |> Seq.head |> fun (x:{| sum:int option |}) -> x.sum
        Expect.isSome result ""
        Expect.equal 1 result.Value ""
    }

    testTask "Select aggregate min with empty table" {
        do! Persons.init conn
        let! fromDb =
            select {
                table "Persons"
                where (eq "Position" 1)
            } |> fun query -> conn.SelectAsync<{| min:int option |}>(query, aggr = [ min "Position" "min" ])
        let result = fromDb |> Seq.head |> fun (x:{| min:int option |}) -> x.min
        Expect.isNone result ""
    }

    testTask "Select aggregate min" {
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
            } |> fun query -> conn.SelectAsync<{| min:int option |}>(query, aggr = [ min "Position" "min" ])
        let result = fromDb |> Seq.head |> fun (x:{| min:int option |}) -> x.min
        Expect.isSome result ""
        Expect.equal 1 result.Value ""
    }

    testTask "Select aggregate min with where condition" {
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
                where (eq "Position" 2) // Set the value not to the smallest one like others to make sure min works
            } |> fun query -> conn.SelectAsync<{| min:int option |}>(query, aggr = [ min "Position" "min" ])
        let result = fromDb |> Seq.head |> fun (x:{| min:int option |}) -> x.min
        Expect.isSome result ""
        Expect.equal 2 result.Value ""
    }

    testTask "Select aggregate max with empty table" {
        do! Persons.init conn
        let! fromDb =
            select {
                table "Persons"
            } |> fun query -> conn.SelectAsync<{| max:int option |}>(query, aggr = [ max "Position" "max" ])
        let result = fromDb |> Seq.head |> fun (x:{| max:int option |}) -> x.max
        Expect.isNone result ""
    }

    testTask "Select aggregate max" {
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
            } |> fun query -> conn.SelectAsync<{| max:int option |}>(query, aggr = [ max "Position" "max" ])
        let result = fromDb |> Seq.head |> fun (x:{| max:int option |}) -> x.max
        Expect.isSome result ""
        Expect.equal 10 result.Value ""
    }

    testTask "Select aggregate max with where condition" {
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
                where (eq "Position" 1)
            } |> fun query -> conn.SelectAsync<{| max:int option |}>(query, aggr = [ max "Position" "max" ])
        let result = fromDb |> Seq.head |> fun (x:{| max:int option |}) -> x.max
        Expect.isSome result ""
        Expect.equal 1 result.Value ""
    }

    testTask "Select aggregate multiple functions" {
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
            } |> fun query ->
                conn.SelectAsync<{| count:int; avg:int option; sum:int option; max:int option; min:int option |}>(
                    query, [ avg "Position" "avg"; count "*" "count"; max "Position" "max"; min "Position" "min"; sum "Position" "sum" ])
        let count, avg, sum, max, min = 
            fromDb 
            |> Seq.head 
            |> fun (x:{| count:int; avg:int option; sum:int option; max:int option; min:int option |}) -> 
                x.count, x.avg, x.sum, x.max, x.min
        Expect.isSome avg ""
        Expect.isSome sum ""
        Expect.isSome min ""
        Expect.isSome max ""

        Expect.equal 10 count ""
        Expect.equal 5 avg.Value ""
        Expect.equal 55 sum.Value ""
        Expect.equal 1 min.Value ""
        Expect.equal 10 max.Value ""
    }

    testTask "Select aggregate multiple same function types" {
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
            } |> fun query ->
                conn.SelectAsync<{| countPositions1:int; countPositions2:int |}>(
                    query, [ count "Position" "countPositions1"; count "Position" "countPositions2" ])
        let pos1, pos2 = 
            fromDb 
            |> Seq.head 
            |> fun (x:{| countPositions1:int; countPositions2:int |}) -> x.countPositions1, x.countPositions2

        Expect.equal 10 pos1 ""
        Expect.equal 10 pos2 ""
    }

    testTask "Select aggregate with inner joined query" {
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
            } |> fun query ->
                conn.SelectAsync<{| count:int |}>(query, [ count "*" "count" ])
        
        let result = fromDb |> Seq.head |> fun (x:{| count:int |}) -> x
        Expect.equal 5 result.count ""
    }

    testTask "Select aggregate with left joined query" {
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
            } |> fun query ->
                conn.SelectAsync<{| count:int |}>(query, [ count "*" "count" ])
        
        let result = fromDb |> Seq.head |> fun (x:{| count:int |}) -> x
        Expect.equal 14 result.count ""
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

        let! fromDb =
            select {
                table "Persons"
                leftJoin "Dogs" "OwnerId" "Persons.Id"
                groupBy [ "Persons.Position" ]
            } |> fun query ->
                conn.SelectAsync<{| position:int; count:int |}>(query, [ count "*" "count" ])
        
        fromDb 
        |> Seq.iter (fun (x:{| position:int; count:int |}) -> 
            if x.position = 1 // Only head has dogs created
            then Expect.equal 5 x.count ""
            else Expect.equal 1 x.count ""
        )
    }

    testTask "Select group by aggregate with tupled result" {
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
                groupBy [ "Persons.Position"; "Dogs.OwnerId" ]
            } |> fun query ->
                conn.SelectAsync<{| position:int; count:int |}, {| ownerId:System.Guid |}>(query, [ count "Persons.Position" "count" ])
        
        fromDb 
        |> Seq.iter (fun (a:{| position:int; count:int |}, _) -> 
            if a.position = 1 // Only head has dogs created
            then Expect.equal 5 a.count ""
            else Expect.equal 1 a.count ""
        )
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

        let! fromDb =
            select {
                table "Persons"
                distinct
                leftJoin "Dogs" "OwnerId" "Persons.Id"
            } |> conn.SelectAsync<{| firstName:string |}>
        
        Expect.equal 10 (fromDb |> Seq.length) ""
        Expect.equal 10 (fromDb |> Seq.distinctBy (fun (x:{| firstName:string |}) -> x.firstName) |> Seq.length) ""
    }
]