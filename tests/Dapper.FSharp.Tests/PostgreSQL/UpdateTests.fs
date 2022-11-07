module Dapper.FSharp.Tests.PostgreSQL.UpdateTests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Dapper.FSharp.PostgreSQL
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

            Assert.ThrowsAsync<OperationCanceledException>(action) |> ignore
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

[<TestFixture>]
[<NonParallelizable>]
type UpdateOutputTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit

    [<Test>]
    member _.``Updates option field to Some``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                update {
                    for p in personsView do
                    setColumn p.DateOfBirth (Some DateTime.UtcNow)
                    where (p.Position = 2)
                } |> conn.UpdateOutputAsync
            
            Assert.IsTrue(fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth |> Option.isSome)
            Assert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }
    
    [<Test>]
    member _.``Updates and outputs single record``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position = 2)
                } |> conn.UpdateOutputAsync
                
            Assert.AreEqual("UPDATED", fromDb |> Seq.head |> fun (x:Persons.View) -> x.LastName)
            Assert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }

    [<Test>]
    member _.``Updates and outputs multiple records``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! insertedPersonIds =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertOutputAsync<Persons.View, {| Id:Guid |}>
            let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:Guid |}) -> p.Id) |> Seq.toList
            let! updated =
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (isIn p.Id personIds)
                } |> conn.UpdateOutputAsync // If we specify the output type after, we dont need to specify it here
            
            Assert.AreEqual(10, Seq.length updated)
            updated |> Seq.iter (fun (p:Persons.View) -> // Output specified here
                Assert.AreEqual("UPDATED", p.LastName)
                Assert.IsTrue (personIds |> List.exists ((=) p.Id))
            )
        }

    [<Test>]
    member _.``Updates and outputs subset of single record columns``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position = 2)
                } |> conn.UpdateOutputAsync
            let pos2Id = rs |> List.pick (fun p -> if p.Position = 2 then Some p.Id else None)
            
            Assert.AreEqual(pos2Id, fromDb |> Seq.head |> fun (p:{| Id:Guid |}) -> p.Id)
        }

    [<Test>]
    member _.``Updates option field to None and outputs record``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some DateTime.UtcNow })
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                update {
                    for p in personsView do
                    setColumn p.DateOfBirth None
                    where (p.Position = 2)
                } |> conn.UpdateOutputAsync
            
            Assert.IsTrue (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth |> Option.isNone)
            Assert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }

    [<Test>]
    member _.``Updates option field to Some and outputs record``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                update {
                    for p in personsView do
                    setColumn p.DateOfBirth (Some DateTime.UtcNow)
                    where (p.Position = 2)
                } |> conn.UpdateOutputAsync
            
            Assert.IsTrue (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth |> Option.isSome)
            Assert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }