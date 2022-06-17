module Dapper.FSharp.Tests.IssuesTests

open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto
open Dapper.FSharp.Tests.Extensions

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "Issues" [
    
    let personsView = table'<Persons.View> "Persons" // |> inSchema "dbo"
    let dogsView = table'<Dogs.View> "Dogs" //|> inSchema "dbo"
    
    ftestTask "Select with join over constant #62" {
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
            } |> crud.InsertAsync
        let! _ =
            insert {
                into dogsView
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                innerJoin d in dogsView on ((p.Id, thirdPersonId) = (d.OwnerId,d.OwnerId))
                selectAll
            } |> crud.SelectAsync<Persons.View, Dogs.View>
        
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal (thirdPerson, thirdDog) (Seq.head fromDb) ""
    }
]

//let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "Issues OUTPUT" [
//    testTask "Returns auto-increment value back" {
//        do! init.InitArticles()
//        let! ins =
//            insert {
//                table "Articles"
//                value ({| Title = "MyTitle" |})
//            } |> crud.InsertOutputAsync<{| Title : string |}, {| Id : int |}>
//        let lastInserted = ins |> Seq.head |> (fun (x:{| Id : int |}) -> x.Id)
//        Expect.equal 1 lastInserted ""
//    }
//]