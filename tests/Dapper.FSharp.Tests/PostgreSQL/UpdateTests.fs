module Dapper.FSharp.Tests.PostgreSQL.UpdateTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.PostgreSQL.Database
open Dapper.FSharp
open Dapper.FSharp.PostgreSQL

let tests (conn:IDbConnection) = Tests.testList "UPDATE" [

    testTask "Updates single records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (eq "Position" 2)
            } |> conn.UpdateAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "LastName" "UPDATED")
            } |> conn.SelectAsync<Persons.View>
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates option field to None" {
        do! Persons.init conn
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table "Persons"
                set {| DateOfBirth = None |}
                where (eq "Position" 2)
            } |> conn.UpdateAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Position" 2)
            } |> conn.SelectAsync<Persons.View>
        Expect.isNone (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates option field to Some" {
        do! Persons.init conn
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table "Persons"
                set {| DateOfBirth = Some System.DateTime.UtcNow |}
                where (eq "Position" 2)
            } |> conn.UpdateAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Position" 2)
            } |> conn.SelectAsync<Persons.View>    
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates more records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (gt "Position" 7)
            } |> conn.UpdateAsync

        let! fromDb =
            select {
                table "Persons"
                where (eq "LastName" "UPDATED")
            } |> conn.SelectAsync<Persons.View>
        Expect.equal 3 (Seq.length fromDb) ""
    }


    
]


 