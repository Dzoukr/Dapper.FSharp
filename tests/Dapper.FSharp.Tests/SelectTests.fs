module Dapper.FSharp.Tests.SelectTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto
open FSharp.Control.Tasks.V2

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "SELECT" [

    testTask "Selects by single where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Position" 5)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
    }

    testTask "Selects by single where condition with table name used" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Persons.Position" 5)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
    }

    testTask "Selects by IN where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (isIn "Position" [5;6])
                orderBy "Position" Asc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
        Expect.equal (rs |> List.find (fun x -> x.Position = 6)) (Seq.last fromDb) ""
    }

    testTask "Selects by NOT IN where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (isNotIn "Position" [1;2;3])
                orderBy "Position" Asc
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
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (isNullValue "DateOfBirth")
                orderBy "Position" Asc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 2)) (Seq.head fromDb) ""
        Expect.equal 5 (Seq.length fromDb) ""
    }

    testTask "Selects by IS NOT NULL where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (isNotNullValue "DateOfBirth")
                orderBy "Position" Asc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 1)) (Seq.head fromDb) ""
        Expect.equal 5 (Seq.length fromDb) ""
    }

    testTask "Selects by LIKE where condition return matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (like "FirstName" "First_1%")
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
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (notLike "FirstName" "First_1%")
            }
            |> crud.SelectAsync<Persons.View>
        Expect.hasLength fromDb 8 ""
    }

    testTask "Selects by NOT LIKE where condition do not return non-matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (notLike "FirstName" "NonExistingName%")
            } |> crud.SelectAsync<Persons.View>
        Expect.hasLength fromDb 10 ""
    }

    testTask "Selects by UNARY NOT where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where !! (gt "Position" 5 + isNullValue "DateOfBirth")
                orderBy "Position" Asc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 9)) (Seq.last fromDb) ""
        Expect.equal 7 (Seq.length fromDb) ""
    }

    testTask "Selects by multiple where conditions" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                where (gt "Position" 2 + lt "Position" 4)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 3)) (Seq.head fromDb) ""
    }

    testTask "Selects with order by" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                orderBy "Position" Desc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 10 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
    }

    testTask "Selects with skip parameter" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                skip 5
                orderBy "Position" Asc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 5 (fromDb |> Seq.length) ""
    }

    testTask "Selects with skipTake parameter" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                skipTake 5 2
                orderBy "Position" Asc
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 2 (fromDb |> Seq.length) ""
    }

    testTask "Selects with skip and take parameters" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                skip 5
                take 2
                orderBy "Position" Asc
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
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                innerJoin "Dogs" "OwnerId" "Persons.Id"
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
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                innerJoin "Dogs" "OwnerId" "Persons.Id"
            } |> crud.SelectAsync<Persons.View, Dogs.View>

        let byOwner = fromDb |> Seq.groupBy fst

        Expect.equal 5 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
        Expect.equal 1 (Seq.length byOwner) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
    }

    testTask "Selects with one inner join on two columns - 1:1" {
        do! init.InitPersons()
        do! init.InitDogs()
        do! init.InitVaccinationHistory()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1to1 persons
        let vaccinations = DogVaccinationHistory.View.generate1to1 dogs

        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync

        let! _ =
            insert {
                table "VaccinationHistory"
                values vaccinations
            } |> crud.InsertAsync

        let! fromDb =
            select {
                table "Dogs"
                innerJoin "VaccinationHistory" ["PetOwnerId", "Dogs.OwnerId"; "DogNickname", "Dogs.Nickname"]
                orderByMany [OrderBy ("Dogs.Nickname", Asc); OrderBy ("VaccinationDate", Desc)]
            } |> crud.SelectAsync<Dogs.View>

        Expect.equal 10 (Seq.length fromDb) "Expecting 10 records from db"
        Expect.equal dogs.Head (Seq.head fromDb) "First record should match."
    }

    testTask "Selects with one inner join on 2 columns - 1:N" {
        do! init.InitPersons()
        do! init.InitDogs()
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let vaccinations = DogVaccinationHistory.View.generate1toN 20 dogs.Head
        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "VaccinationHistory"
                values vaccinations
            } |> crud.InsertAsync

        let! fromDb =
            select {
                table "Dogs"
                innerJoin "VaccinationHistory" ["PetOwnerId", "Dogs.OwnerId"; "DogNickname", "Dogs.Nickname"]
                orderByMany ["Dogs.Nickname", Asc; "VaccinationHistory.VaccinationDate", Desc]
            } |> crud.SelectAsync<Dogs.View, DogVaccinationHistory.View>

        let byDog = fromDb |> Seq.groupBy fst

        Expect.equal (Seq.length fromDb) 20 ""
        Expect.equal (Seq.length byDog) 1 ""
        Expect.equal (byDog |> Seq.head |> snd |> Seq.length) 20 ""
        Expect.equal (Seq.head fromDb) (dogs.Head, vaccinations.Head) "First record from db matches generated data"
    }
    testTask "Selects with one left join" {
        do! init.InitPersons()
        do! init.InitDogs()
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                table "Persons"
                leftJoin "Dogs" "OwnerId" "Persons.Id"
                orderByMany ["Persons.Position", Asc; "Dogs.Nickname", Asc]
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
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                table "Persons"
                innerJoin "Dogs" "OwnerId" "Persons.Id"
                innerJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
                orderBy "Persons.Position" Asc
            } |> crud.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>

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
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                table "Persons"
                innerJoin "Dogs" "OwnerId" "Persons.Id"
                innerJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
                orderByMany ["Persons.Position", Asc; "Dogs.Nickname", Asc; "DogsWeights.Year", Asc]
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
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                table "Persons"
                leftJoin "Dogs" "OwnerId" "Persons.Id"
                leftJoin "DogsWeights" "DogNickname" "Dogs.Nickname"
                orderByMany ["Persons.Position", Asc; "Dogs.Nickname", Asc; "DogsWeights.Year", Asc]
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