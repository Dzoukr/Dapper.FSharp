module Dapper.FSharp.Tests.SelectTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Builders
open Dapper.FSharp.Builders.Operators
open Expecto
open FSharp.Control.Tasks.V2
open System.Threading
open Dapper.FSharp.Tests.Extensions

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "SELECT" [

    let personsView = table'<Persons.View> "Persons" // |> inSchema "dbo"
    let dogsView = table'<Dogs.View> "Dogs" //|> inSchema "dbo"
    let dogsWeightsView = table'<DogsWeights.View> "DogsWeights" // |> inSchema "dbo"
    
    testTask "Selects by single where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Position = 5)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
    }

    testTask "Cancellation" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync

        use cts = new CancellationTokenSource()
        cts.Cancel()
        let selectCrud query =
            crud.SelectAsync<Persons.View>(query, cancellationToken = cts.Token) :> Task
        let action () = 
            select {
                for p in personsView do
                where (p.Position = 5)
            } |> selectCrud
        do! Expect.throwsTaskCanceledException action "Should be canceled action"
    }


    testTask "Selects by single where condition with table name used" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Position = 5)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
    }

    testTask "Selects by IN where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (isIn p.Position [5;6])
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
        Expect.equal (rs |> List.find (fun x -> x.Position = 6)) (Seq.last fromDb) ""
    }

    testTask "Selects by NOT IN where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (isNotIn p.Position [1;2;3])
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 4)) (Seq.head fromDb) ""
        Expect.equal (rs |> List.find (fun x -> x.Position = 10)) (Seq.last fromDb) ""
        Expect.equal 7 (Seq.length fromDb) ""
    }

    testTask "Selects by IS NULL where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.DateOfBirth = None)
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 2)) (Seq.head fromDb) ""
        Expect.equal 5 (Seq.length fromDb) ""
    }

    testTask "Selects by IS NOT NULL where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.DateOfBirth <> None)
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 1)) (Seq.head fromDb) ""
        Expect.equal 5 (Seq.length fromDb) ""
    }

    testTask "Selects by LIKE where condition return matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (like p.FirstName "First_1%")
            } |> crud.SelectAsync<Persons.View>
        Expect.isNonEmpty fromDb ""
        Expect.hasLength fromDb 2 ""
        Expect.isTrue (fromDb |> Seq.forall (fun (p:Persons.View) -> p.FirstName.StartsWith "First")) ""
    }
    
    testTask "Selects by NOT LIKE where condition return matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (notLike p.FirstName "First_1%")
            }
            |> crud.SelectAsync<Persons.View>
        Expect.hasLength fromDb 8 ""
    }

    testTask "Selects by NOT LIKE where condition do not return non-matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (notLike p.FirstName "NonExistingName%")
            } |> crud.SelectAsync<Persons.View>
        Expect.hasLength fromDb 10 ""
    }

    testTask "Selects by UNARY NOT where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (not(p.Position > 5 && p.DateOfBirth = None))
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 9)) (Seq.last fromDb) ""
        Expect.equal 7 (Seq.length fromDb) ""
    }

    testTask "Selects by multiple where conditions" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Position > 2 && p.Position < 4)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 3)) (Seq.head fromDb) ""
    }

    testTask "Selects with order by" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                orderByDescending p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 10 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
    }

    testTask "Selects with skip parameter" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                skip 5
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 5 (fromDb |> Seq.length) ""
    }

    testTask "Selects with skipTake parameter" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                skipTake 5 2
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 2 (fromDb |> Seq.length) ""
    }
    
    testTask "Selects with skip and take parameters" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                skip 5
                take 2
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 2 (fromDb |> Seq.length) ""
    }

    testTask "Selects with one inner join - 1:1" {
        do! init.InitPersons()
        do! init.InitDogs()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1to1 persons
        let! _ =
            insert {
                into personsView
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsView
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                innerJoin d in dogsView on (p.Id = d.OwnerId)
                selectAll
            } |> crud.SelectAsync<Persons.View, Dogs.View>

        Expect.equal 10 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
    }

    testTask "Selects with one inner join - 1:N" {
        do! init.InitPersons()
        do! init.InitDogs()
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                into personsView
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsView
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                innerJoin d in dogsView on (p.Id = d.OwnerId)
                selectAll
            } |> crud.SelectAsync<Persons.View, Dogs.View>

        let byOwner = fromDb |> Seq.groupBy fst

        Expect.equal 5 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
        Expect.equal 1 (Seq.length byOwner) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
    }

    testTask "Selects with one left join" {
        do! init.InitPersons()
        do! init.InitDogs()
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                into personsView
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsView
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                leftJoin d in dogsView on (p.Id = d.OwnerId)
                orderBy p.Position
                thenBy d.Nickname
            } |> crud.SelectAsyncOption<Persons.View, Dogs.View>

        let byOwner = fromDb |> Seq.groupBy fst

        Expect.equal 14 (Seq.length fromDb) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
        Expect.isTrue (fromDb |> Seq.last |> snd |> Option.isNone) ""
        Expect.equal (dogs |> List.head |> Some) (fromDb |> Seq.head |> snd) ""
    }

    testTask "Selects with two inner joins - 1:1" {
        do! init.InitPersons()
        do! init.InitDogs()
        do! init.InitDogsWeights()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1to1 persons
        let weights = DogsWeights.View.generate1to1 dogs

        let! _ =
            insert {
                into personsView
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsView
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsWeightsView
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                for p in personsView do
                innerJoin d in dogsView on (p.Id = d.OwnerId)
                innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                orderBy p.Position
            }
            |> crud.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>

        Expect.equal 10 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head, weights.Head) (Seq.head fromDb) ""
    }

    testTask "Selects with two inner joins - 1:N" {
        do! init.InitPersons()
        do! init.InitDogs()
        do! init.InitDogsWeights()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let weights = DogsWeights.View.generate1toN 3 dogs.Head

        let! _ =
            insert {
                into personsView
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsView
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsWeightsView
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                for p in personsView do
                innerJoin d in dogsView on (p.Id = d.OwnerId)
                innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                orderBy p.Position
                thenBy d.Nickname
                thenBy dw.Year
            } |> crud.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>

        Expect.equal 3 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head, weights.Head) (Seq.head fromDb) ""
    }

    testTask "Selects with two left joins" {
        do! init.InitPersons()
        do! init.InitDogs()
        do! init.InitDogsWeights()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let weights = DogsWeights.View.generate1toN 3 dogs.Head

        let! _ =
            insert {
                into personsView
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsView
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsWeightsView
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                for p in personsView do
                leftJoin d in dogsView on (p.Id = d.OwnerId)
                leftJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                orderBy p.Position
                thenBy d.Nickname
                thenBy dw.Year
            } |> crud.SelectAsyncOption<Persons.View, Dogs.View, DogsWeights.View>

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