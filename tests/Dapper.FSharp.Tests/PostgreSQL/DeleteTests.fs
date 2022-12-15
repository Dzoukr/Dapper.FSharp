module Dapper.FSharp.Tests.PostgreSQL.DeleteTests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Dapper.FSharp.PostgreSQL
open Dapper.FSharp.Tests.Database

[<TestFixture>]
[<NonParallelizable>]
type DeleteTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Deletes single records``() =
        task {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> conn.DeleteAsync
        let! fromDb =
            select {
                for p in personsView do
                orderByDescending p.Position
            } |> conn.SelectAsync<Persons.View>
        
        Assert.AreEqual(9, Seq.length fromDb)
        Assert.AreEqual(9, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
    }
    
    [<Test>]
    member _.`` Cancellation works``() =
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
        let deleteCrud query =
            conn.DeleteAsync(query, cancellationToken = cts.Token) :> Task
        let action () = 
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> deleteCrud 
        
        Assert.ThrowsAsync<OperationCanceledException>(action) |> ignore
    }
    
    [<Test>]
    member _.``Deletes more records``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                delete {
                    for p in personsView do
                    where (p.Position >= 7)
                } |> conn.DeleteAsync

            let! fromDb =
                select {
                    for p in personsView do
                    selectAll
                } |> conn.SelectAsync<Persons.View>
            
            Assert.AreEqual(6, Seq.length fromDb)
        }
        
[<TestFixture>]
[<NonParallelizable>]
type DeleteOutputTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Deletes and outputs single record``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                delete {
                    for p in personsView do
                    where (p.Position = 10)
                } |> conn.DeleteOutputAsync<Persons.View>
            
            Assert.AreEqual(1, Seq.length fromDb)
            Assert.AreEqual(10, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }
    
    [<Test>]
    member _.``Deletes and outputs multiple records``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! insertedPersonIds =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertOutputAsync
            let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:System.Guid |}) -> p.Id) |> Seq.toList

            let! deleted =
                delete {
                    for p in personsView do
                    where (isIn p.Id personIds)
                } |> conn.DeleteOutputAsync<Persons.View>
            
            Assert.AreEqual(10, Seq.length deleted)
            deleted |> Seq.iter (fun (p:Persons.View) ->
                Assert.IsTrue (personIds |> List.exists ((=) p.Id)))
        }