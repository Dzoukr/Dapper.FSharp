﻿module Dapper.FSharp.Tests.PostgreSQL.IssuesTests

open NUnit.Framework
open Dapper.FSharp.PostgreSQL
open Dapper.FSharp.Tests.Database

[<TestFixture>]
[<NonParallelizable>]
type IssuesTests () =
    let personsView = table'<Persons.View> "Persons"
    let dogsView = table'<Dogs.View> "Dogs"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Select with inner join over constant #62``() = 
        task {
            do! init.InitPersons()
            do! init.InitDogs()

            let persons = Persons.View.generate 10
            let dogs = Dogs.View.generate1to1 persons
            
            let thirdPerson = persons.[2]
            let thirdDog = dogs.[2]
            let thirdPersonId = thirdPerson.Id
            let! _ =
                insert {
                    into personsView
                    values persons
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values dogs
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on ((p.Id, thirdPersonId) = (d.OwnerId,d.OwnerId))
                    selectAll
                } |> conn.SelectAsync<Persons.View, Dogs.View>
            
            Assert.AreEqual(1, Seq.length fromDb)
            Assert.AreEqual((thirdPerson, thirdDog), (Seq.head fromDb))
        }
    
    [<Test>]
    member _.``Select with left join over constant #62``() = 
        task {
            do! init.InitPersons()
            do! init.InitDogs()

            let persons = Persons.View.generate 10
            let secondPerson = persons.[1]
            let secondPersonId = secondPerson.Id
            
            let dogs = Dogs.View.generate1toN 5 secondPerson
            
            let! _ =
                insert {
                    into personsView
                    values persons
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values dogs
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on ((p.Id, secondPersonId) = (d.OwnerId,d.OwnerId))
                    selectAll
                } |> conn.SelectAsyncOption<Persons.View, Dogs.View>
            
            Assert.AreEqual(14, Seq.length fromDb)
        }
    
    [<Test>]
    member _.``Support for ilike in postgresql #65``() = 
        task {
            do! init.InitPersons()

            let persons = Persons.View.generate 10
            
            let! _ =
                insert {
                    into personsView
                    values persons
                } |> conn.InsertAsync
          
            let! fromDb =
                select {
                    for p in personsView do
                    where (ilike p.FirstName "fIrSt_%")
                } |> conn.SelectAsync<Persons.View>
            
            Assert.AreEqual(10, Seq.length fromDb)
        }
    
    [<Test>]
    member _.``Support for not ilike in postgresql #65``() = 
        task {
            do! init.InitPersons()

            let persons = Persons.View.generate 10
            
            let! _ =
                insert {
                    into personsView
                    values persons
                } |> conn.InsertAsync
          
            let! fromDb =
                select {
                    for p in personsView do
                    where (notILike p.FirstName "fIrSt_%")
                } |> conn.SelectAsync<Persons.View>
            
            Assert.AreEqual(0, Seq.length fromDb)
        }