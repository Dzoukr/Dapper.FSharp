module Dapper.FSharp.Tests.MSSQL.UpdateTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL

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
                where (eq "LastName" "UPDATED")
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
        let! fromDb =
            update {
                table "Persons"
                set {| DateOfBirth = Some System.DateTime.UtcNow |}
                where (eq "Position" 2)
            } |> conn.UpdateOutputAsync
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


    /// OUTPUT tests

    testTask "Updates and outputs single record" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (eq "Position" 2)
            } |> conn.UpdateOutputAsync<{| LastName:string |}, Persons.View> // Example how to explicitly declare types
        Expect.equal "UPDATED" (fromDb |> Seq.head |> fun (x:Persons.View) -> x.LastName) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates and outputs multiple records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! insertedPersonIds =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertOutputAsync<Persons.View, {| Id:System.Guid |}>
        let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:System.Guid |}) -> p.Id) |> Seq.toList
        let boxedPersonIds = personIds |> List.map box |> Seq.toList
        let! updated =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (isIn "Id" boxedPersonIds)
            } |> conn.UpdateOutputAsync // If we specify the output type after, we dont need to specify it here
        Expect.hasLength updated 10 ""
        updated |> Seq.iter (fun (p:Persons.View) -> // Output specified here
            Expect.equal "UPDATED" (p.LastName) ""
            Expect.isTrue (personIds |> List.exists ((=) p.Id)) "Updated personId not found from inserted Ids")
    }

    testTask "Updates and outputs subset of single record columns" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| LastName = "UPDATED" |}
                where (eq "Position" 2)
            } |> conn.UpdateOutputAsync
        let pos2Id = rs |> List.pick (fun p -> if p.Position = 2 then Some p.Id else None)
        Expect.equal pos2Id (fromDb |> Seq.head |> fun (p:{| Id:System.Guid |}) -> p.Id) ""
    }

    testTask "Updates option field to None and outputs record" {
        do! Persons.init conn
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some System.DateTime.UtcNow })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| DateOfBirth = None |}
                where (eq "Position" 2)
            } |> conn.UpdateOutputAsync
        Expect.isNone (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates option field to Some and outputs record" {
        do! Persons.init conn
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            update {
                table "Persons"
                set {| DateOfBirth = Some System.DateTime.UtcNow |}
                where (eq "Position" 2)
            } |> conn.UpdateOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
]


 