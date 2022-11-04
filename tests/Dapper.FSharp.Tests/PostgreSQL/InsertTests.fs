module Dapper.FSharp.Tests.PostgreSQL.InsertTests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Dapper.FSharp.PostgreSQL
open Dapper.FSharp.Tests.Database

[<TestFixture>]
[<NonParallelizable>]
type InsertTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Inserts new record``() = 
        task {
            do! init.InitPersons()
            let r = Persons.View.generate 1 |> List.head
            let! _ =
                insert {
                    into personsView
                    value r
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.Id = r.Id)
                } |> conn.SelectAsync<Persons.View>
            
            Assert.AreEqual(r, Seq.head fromDb)
        }
    
    [<Test>]
    member _.``Cancellation works``() = 
        task {
            do! init.InitPersons()
            let r = Persons.View.generate 1 |> List.head

            use cts = new CancellationTokenSource()
            cts.Cancel()
            let insertCrud query =
                conn.InsertAsync(query, cancellationToken = cts.Token) :> Task
            let action () = 
                insert {
                    into personsView
                    value r
                } |> insertCrud 
            
            Assert.ThrowsAsync<OperationCanceledException>(action) |> ignore
        }
    
    [<Test>]
    member _.``Inserts partial record``() = 
        task {        
            let personsRequired = table'<Persons.ViewRequired> "Persons"

            do! init.InitPersons()
            let r =
                Persons.View.generate 1
                |> List.head
                |> fun x -> ({ Id = x.Id; FirstName = x.FirstName; LastName = x.LastName; Position = x.Position } : Persons.ViewRequired)
            let! _ =
                insert {
                    into personsRequired
                    value r
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsRequired do
                    where (p.Id = r.Id)
                } |> conn.SelectAsync<Persons.ViewRequired>
            
            Assert.AreEqual(r, Seq.head fromDb)
        }
    
    [<Test>]
    member _.``Inserts partial record using 'excludeColumn'``() = 
        task {        
            let personsView = table'<Persons.View> "Persons"

            do! init.InitPersons()
            let r =
                Persons.View.generate 1
                |> List.head
            let! _ =
                insert {
                    for p in personsView do
                    value r
                    excludeColumn r.DateOfBirth
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.Id = r.Id)
                } |> conn.SelectAsync<Persons.View>
            
            Assert.AreEqual({ r with DateOfBirth = None }, Seq.head fromDb)
        }
    
    [<Test>]
    member _.``Inserts more records``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            CollectionAssert.AreEqual(rs, Seq.toList fromDb)
        }
    
    [<Test>]
    member _.``Insert with 2 included fields``() = 
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
                insert {
                    for p in table<Persons.View> do
                    value person
                    includeColumn p.FirstName
                    includeColumn p.LastName
                }
                
            Assert.AreEqual (query.Fields, [nameof(person.FirstName); nameof(person.LastName)])
        }


[<TestFixture>]
[<NonParallelizable>]
type InsertOutputTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Inserts and outputs single record``() = 
        task {
            do! init.InitPersons()
            let r = Persons.View.generate 1 |> List.head
            let! fromDb =
                insert {
                    into personsView
                    value r
                } |> conn.InsertOutputAsync<Persons.View, Persons.View> // Optional type specification
            
            Assert.AreEqual(r, Seq.head fromDb)
        }
    
    [<Test>]
    member _.``Inserts and outputs multiple records``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! insertedPersons =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertOutputAsync
            let generatedPositions = rs |> List.map (fun p -> p.Position)
            
            Assert.AreEqual(10, Seq.length insertedPersons)
            insertedPersons |> Seq.iter (fun (p:Persons.View) ->
                Assert.IsTrue (generatedPositions |> List.exists ((=) p.Position))
            )
        }
    
    [<Test>]
    member _.``Inserts and outputs subset of single record columns``() = 
        task {
            do! init.InitPersons()
            let r = Persons.View.generate 1 |> List.head
            let! fromDb =
                insert {
                    into personsView
                    value r
                } |> conn.InsertOutputAsync
            
            Assert.AreEqual(r.Position, Seq.head fromDb |> fun (p:{| Position:int |}) -> p.Position)
        }
    
    [<Test>]
    member _.``Inserts row with None value and outputs record``() = 
        task {
            do! init.InitPersons()
            let r = Persons.View.generate 1 |> List.head |> fun p -> { p with DateOfBirth = None }
            let! (fromDb:seq<Persons.View>)=
                insert {
                    into personsView
                    value r
                } |> conn.InsertOutputAsync
            
            Assert.AreEqual(r.Id, fromDb |> Seq.head |> (fun x -> x.Id))
        }
    
    [<Test>]
    member _.``Inserts row with Some value and outputs record``() = 
        task {
            do! init.InitPersons()
            let r = Persons.View.generate 1 |> List.head |> fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow }
            let! fromDb =
                insert {
                    into personsView
                    value r
                } |> conn.InsertOutputAsync
            
            Assert.IsTrue(fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth |> Option.isSome)
            Assert.AreEqual(r.Id, Seq.head fromDb |> fun (p:Persons.View) -> p.Id) // Comparing Some <datetime> fails even though it is the same
        }