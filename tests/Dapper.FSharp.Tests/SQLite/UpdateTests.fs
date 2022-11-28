module Dapper.FSharp.Tests.SQLite.UpdateTests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Dapper.FSharp.SQLite
open Dapper.FSharp.Tests.Database

[<TestFixture>]
[<NonParallelizable>]
type UpdateTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Updates single records``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position = 2)
                } |> conn.UpdateAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.LastName = "UPDATED")
                } |> conn.SelectAsync<Persons.View>
            
            Assert.AreEqual(1, Seq.length fromDb)
            Assert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }

    [<Test>]
    member _.``Cancellation works``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync

            use cts = new CancellationTokenSource()
            cts.Cancel()
            let updateCrud query =
                conn.UpdateAsync(query, cancellationToken = cts.Token) :> Task
            let action () = 
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position = 2)
                } |> updateCrud

            Assert.ThrowsAsync<TaskCanceledException>(action) |> ignore
        }

    [<Test>]
    member _.``Updates option field to None``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some DateTime.UtcNow })
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                update {
                    for p in personsView do
                    setColumn p.DateOfBirth None
                    where (p.Position = 2)
                } |> conn.UpdateAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.Position = 2)
                } |> conn.SelectAsync<Persons.View>
            
            Assert.IsTrue(fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth |> Option.isNone)
            Assert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }

    [<Test>]
    member _.``Updates more records``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position > 7)
                } |> conn.UpdateAsync

            let! fromDb =
                select {
                    for p in personsView do
                    where (p.LastName = "UPDATED")
                } |> conn.SelectAsync<Persons.View>
            
            Assert.AreEqual(3, Seq.length fromDb)
        }
    
    [<Test>]
    member _.``Update with 2 included fields``() = 
        task {
            let person = 
                {
                    Id = Guid.Empty
                    FirstName = "John"
                    LastName = "Doe"
                    Position = 100
                    DateOfBirth = None
                } : Persons.View
        
            let query =
                update {
                    for p in table<Persons.View> do
                    set person
                    includeColumn p.FirstName
                    includeColumn p.LastName
                }
                
            Assert.AreEqual(query.Fields, [nameof(person.FirstName); nameof(person.LastName)])
        }
