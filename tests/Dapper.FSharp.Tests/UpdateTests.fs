﻿module Dapper.FSharp.Tests.UpdateTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto
open FSharp.Control.Tasks.V2

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "UPDATE" [
    
    testTask "Updates single records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! _ =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (eq "Position" 2)
            } |> crud.UpdateAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "LastName" "UPDATED")
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates option field to None" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! _ =
            update {
                table "Persons"
                set {| DateOfBirth = None |}
                where (eq "Position" 2)
            } |> crud.UpdateAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Position" 2)
            } |> crud.SelectAsync<Persons.View>
        Expect.isNone (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates more records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! _ =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (gt "Position" 7)
            } |> crud.UpdateAsync

        let! fromDb =
            select {
                table "Persons"
                where (eq "LastName" "UPDATED")
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 3 (Seq.length fromDb) ""
    }
    
    testTask "Updates values using `excludeColumn`" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow })
        let toUpdate = rs |> List.find (fun x -> x.Position = 2)
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! _ =
            update {
                table "Persons"
                set ({ toUpdate with LastName = "CHANGED"; FirstName = "ALSO CHANGED" })
                where (eq "Position" 2)
                excludeColumn (nameof(toUpdate.Id))
                excludeColumn (nameof(toUpdate.FirstName))
            } |> crud.UpdateAsync
        let! fromDb =
            select {
                table "Persons"
                where (eq "Position" 2)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal "CHANGED" (fromDb |> Seq.head |> fun (x:Persons.View) -> x.LastName) ""
        Expect.equal toUpdate.FirstName (fromDb |> Seq.head |> fun (x:Persons.View) -> x.FirstName) ""
    }
]

let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "UPDATE OUTPUT" [
    
    testTask "Updates option field to Some" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| DateOfBirth = Some System.DateTime.UtcNow |}
                where (eq "Position" 2)
            } |> crud.UpdateOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
    
    testTask "Updates and outputs single record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (eq "Position" 2)
            } |> crud.UpdateOutputAsync<{| LastName:string |}, Persons.View> // Example how to explicitly declare types
        Expect.equal "UPDATED" (fromDb |> Seq.head |> fun (x:Persons.View) -> x.LastName) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates and outputs multiple records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! insertedPersonIds =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertOutputAsync<Persons.View, {| Id:System.Guid |}>
        let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:System.Guid |}) -> p.Id) |> Seq.toList
        let boxedPersonIds = personIds |> List.map box |> Seq.toList
        let! updated =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (isIn "Id" boxedPersonIds)
            } |> crud.UpdateOutputAsync // If we specify the output type after, we dont need to specify it here
        Expect.hasLength updated 10 ""
        updated |> Seq.iter (fun (p:Persons.View) -> // Output specified here
            Expect.equal "UPDATED" (p.LastName) ""
            Expect.isTrue (personIds |> List.exists ((=) p.Id)) "Updated personId not found from inserted Ids")
    }

    testTask "Updates and outputs subset of single record columns" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (eq "Position" 2)
            } |> crud.UpdateOutputAsync
        let pos2Id = rs |> List.pick (fun p -> if p.Position = 2 then Some p.Id else None)
        Expect.equal pos2Id (fromDb |> Seq.head |> fun (p:{| Id:System.Guid |}) -> p.Id) ""
    }

    testTask "Updates option field to None and outputs record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| DateOfBirth = None |}
                where (eq "Position" 2)
            } |> crud.UpdateOutputAsync
        Expect.isNone (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates option field to Some and outputs record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| DateOfBirth = Some System.DateTime.UtcNow |}
                where (eq "Position" 2)
            } |> crud.UpdateOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
]