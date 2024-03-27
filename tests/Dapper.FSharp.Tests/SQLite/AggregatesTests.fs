module Dapper.FSharp.Tests.SQLite.AggregatesTests

open NUnit.Framework
open NUnit.Framework.Legacy
open Dapper.FSharp.SQLite
open Dapper.FSharp.Tests.Database

[<TestFixture>]
[<NonParallelizable>]
type AggregatesTests () =
    let personsView = table'<Persons.View> "Persons"
    let dogsView = table'<Dogs.View> "Dogs"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Selects with COUNT aggregate function``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    count "*" "Value"
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            
            ClassicAssert.AreEqual(10, fromDb.Head.Value)
        }

    [<Test>]
    member _.``Selects with COUNTBY aggregate function``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    countBy p.Id
                }
                |> conn.SelectAsync<{| Id : int |}>
                |> taskToList

            ClassicAssert.AreEqual(10, fromDb.Head.Id)
        }

    [<Test>]
    member _.``Selects with COUNT aggregate function + column``() =
        task {
                do! init.InitPersons()
                let rs =
                    Persons.View.generate 10
                    |> List.map (fun x -> if x.Position > 5 then { x with Position = 10 } else x)
                let! _ =
                    insert {
                        into personsView
                        values rs
                    } |> conn.InsertAsync
                let fromDb =
                    select {
                        for p in personsView do
                        count "*" "Value"
                        groupBy p.Position
                    }
                    |> conn.SelectAsync<{| Value : int; Position : int |}>
                    |> taskToList
                    |> List.rev
                ClassicAssert.AreEqual(6, fromDb.Length)
                ClassicAssert.AreEqual(10, fromDb.Head.Position)
                ClassicAssert.AreEqual(5, fromDb.Head.Value)
            }
        
    [<Test>]
    member _.``Selects with COUNT aggregate function + WHERE``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    count "*" "Value"
                    where (p.Position > 5)
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            
            ClassicAssert.AreEqual(5, fromDb.Head.Value)
        }
        
    [<Test>]
    member _.``Selects with AVG aggregate function``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    avg "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            
            // SQLITE rounds up
            ClassicAssert.AreEqual(6, fromDb.Head.Value)
        }
        
    [<Test>]
    member _.``Selects with SUM aggregate function``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    sum "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            ClassicAssert.AreEqual(55, fromDb.Head.Value)
        }
        
    [<Test>]
    member _.``Selects with MIN aggregate function``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    min "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            ClassicAssert.AreEqual(1, fromDb.Head.Value)
        }
        
    [<Test>]
    member _.``Selects with MAX aggregate function``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    max "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            ClassicAssert.AreEqual(10, fromDb.Head.Value)
        }
        
    [<Test>]
    member _.``Select distinct``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()

            let ps = Persons.View.generate 10
            let ds = Dogs.View.generate1toN 5 ps.Head
            let! _ =
                insert {
                    into personsView
                    values ps
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    distinct
                }
                |> conn.SelectAsync<{| FirstName:string |}>
                |> taskToList

            ClassicAssert.AreEqual(10, fromDb.Length)
        }

    [<Test>]
    member _.``Select countDistinct``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()

            let ps = Persons.View.generate 10
            let ds = Dogs.View.generate1toN 5 ps.Head
            let! _ =
                insert {
                    into personsView
                    values ps
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    countDistinct "Persons.Id" "Value"
                }
                |> conn.SelectAsync<{|Value:int|}>
                |> taskToList

            ClassicAssert.AreEqual(10, fromDb.Head.Value)
        }

    [<Test>]
    member _.``Select countByDistinct``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()

            let ps = Persons.View.generate 10
            let ds = Dogs.View.generate1toN 5 ps.Head
            let! _ =
                insert {
                    into personsView
                    values ps
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    countByDistinct (p.Id)
                }
                |> conn.SelectAsync<{|Id:int|}>
                |> taskToList

            ClassicAssert.AreEqual(10, fromDb.Head.Id)
        }
        
    [<Test>]
    member _.``Selects with multiple aggregate functions``() =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    max "Position" "MaxValue"
                    min "Position" "MinValue"
                }
                |> conn.SelectAsync<{| MaxValue : int; MinValue : int |}>
                |> taskToList

            ClassicAssert.AreEqual(10, fromDb.Head.MaxValue)
            ClassicAssert.AreEqual(1, fromDb.Head.MinValue)
        }
        
    [<Test>]
    member _.``Select group by aggregate``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()

            let px = Persons.View.generate 10
            let ds = Dogs.View.generate1toN 5 px.Head
            let! _ =
                insert {
                    into personsView
                    values px
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let one,two =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    count "Persons.Position" "Count"
                    groupBy (p.Id, p.Position, d.OwnerId)
                    orderBy p.Position
                }
                |> conn.SelectAsync<{| Id: System.Guid; Position:int; Count:int |}, {| OwnerId : System.Guid |}>
                |> taskToList
                |> List.head
                
            ClassicAssert.AreEqual(5, one.Count)
            ClassicAssert.AreEqual(1, one.Position)
            ClassicAssert.AreEqual(one.Id, two.OwnerId)
        }

    [<Test>]
    member _.``Select count inner join, #92``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()

            let px = Persons.View.generate 10
            let ds = Dogs.View.generate1toN 5 px.Head
            let! _ =
                insert {
                    into personsView
                    values px
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    count "*" "Count"
                }
                |> conn.SelectAsync<{| Count:int |}>
                |> taskToList
                |> List.head

            ClassicAssert.AreEqual(5, fromDb.Count)
        }