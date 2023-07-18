module Dapper.FSharp.Tests.MySQL.WhereTests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Dapper.FSharp.MySQL
open Dapper.FSharp.Tests.Database

type CustomDU =
    | PositionBetween of int * int
    | Name of string
    | NoDate

[<TestFixture>]
[<NonParallelizable>]
type WhereTests () =

    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn

    let whereTest selectQuery =
        task {
            do! init.InitPersons()
            let rs = Persons.View.generate 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! fromDb =
                selectQuery |> conn.SelectAsync<Persons.View>
            return fromDb
        }

    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit

    [<TestCase(2)>]
    [<TestCase(7)>]
    [<TestCase(null)>]
    member _.``Selects by where condition with IF`` x = task {
        let o = x |> Option.ofNullable
        let! fromDb =
            whereTest (
                let cond = o.IsSome
                select {
                    for p in personsView do
                    where (if cond then (p.Position > o.Value) else false)
                })

        let expected = 10 - (o |> Option.defaultValue 10)
        Assert.AreEqual (expected, Seq.length fromDb)
        }

    [<TestCase(2)>]
    [<TestCase(7)>]
    [<TestCase(null)>]
    member _.``Selects by where condition with match option`` x = task {
        let o = x |> Option.ofNullable
        let! fromDb =
            whereTest (
                select {
                    for p in personsView do
                    where (match o with | Some x -> p.Position > x | None -> true)
                })

        let expected = 10 - (o |> Option.defaultValue 0)
        Assert.AreEqual (expected, Seq.length fromDb)
        }

    [<TestCase(2, null)>]
    [<TestCase(null, 7)>]
    [<TestCase(2,4)>]
    [<TestCase(5,9)>]
    [<TestCase(7,7)>]
    [<TestCase(null, null)>]
    member _.``Selects by where condition with ANDed match options`` (x1, x2) = task {
        let lb = x1 |> Option.ofNullable
        let ub = x2 |> Option.ofNullable
        let! fromDb =
            whereTest (
                select {
                    for p in personsView do
                    where (
                        (match lb with | Some x -> p.Position > x | None -> true)
                            && (match ub with | Some x -> p.Position < x | None -> true))
                })

        let expected = max 0 ((ub |> Option.defaultValue 11) - (lb |> Option.defaultValue 0) - 1)
        Assert.AreEqual (expected, Seq.length fromDb)
        }

    [<TestCase(2, null)>]
    [<TestCase(null, 7)>]
    [<TestCase(4,2)>]
    [<TestCase(9,5)>]
    [<TestCase(7,3)>]
    [<TestCase(null, null)>]
    member _.``Selects by where condition with ORed match options`` (x1, x2) = task {
        let lb = x1 |> Option.ofNullable
        let ub = x2 |> Option.ofNullable
        let! fromDb =
            whereTest (
                select {
                    for p in personsView do
                    where (
                        (match lb with | Some x -> p.Position > x | None -> false)
                        || (match ub with | Some x -> p.Position < x | None -> false))
                })

        let expected = min 10 (((ub |> Option.defaultValue 1) - 1) + (10 - (lb |> Option.defaultValue 10)))
        Assert.AreEqual (expected, Seq.length fromDb)
        }

    [<TestCase(2)>]
    [<TestCase(7)>]
    [<TestCase(null)>]
    member _.``Selects by where condition with match result`` x = task {
        let o = x |> Option.ofNullable |> Option.map Ok |> Option.defaultValue (Error "error")
        let! fromDb =
            whereTest (
                select {
                    for p in personsView do
                    where (match o with | Ok x -> p.Position > x | Error e -> false)
                })

        let expected = 10 - (o |> Result.defaultValue 10)
        Assert.AreEqual (expected, Seq.length fromDb)
        }

    [<TestCase(2, 7, null, null)>]
    [<TestCase(null, null, "First_1%", null)>]
    [<TestCase(null, null, null, true)>]
    member _.``Selects by where condition with match custom DU`` (p1, p2, n, d) = task {
        let o =
            match Option.ofNullable p1, Option.ofNullable p2, n, d with
            | Some lb, Some ub, _, _ -> PositionBetween (lb, ub)
            | _, _, x, _ when x <> null -> Name x
            | _, _, _, true -> NoDate
            | _ -> failwith "invalid test case"
        let! fromDb =
            whereTest (
                select {
                    for p in personsView do
                    where (match o with | PositionBetween (lb, ub) -> p.Position > lb && p.Position < ub | Name n -> like p.FirstName n | NoDate -> p.DateOfBirth = None)
                })

        match o with
        | PositionBetween (lb, ub) -> Assert.AreEqual (ub - lb - 1, Seq.length fromDb)
        | Name "First_1%" -> Assert.AreEqual (2, Seq.length fromDb)
        | NoDate -> Assert.AreEqual (5, Seq.length fromDb)
        | _ -> Assert.Fail "invalid test case"
        }