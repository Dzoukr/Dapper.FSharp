module Dapper.FSharp.Tests.PostgreSQL.IssuesTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.PostgreSQL.Database
open Dapper.FSharp
open Dapper.FSharp.PostgreSQL

let tests (conn:IDbConnection) = Tests.testList "Issues" [

    testTask "Works with reserved names #8" {
        do! Issues.PersonsSimple.init conn
        let rs = Issues.PersonsSimple.View.generate 10
        let! _ =
            insert {
                table "PersonsSimple"
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table "PersonsSimple"
                set {| Desc = "UPDATED" |}
                where (eq "Id" 5)
            } |> conn.UpdateAsync
        let! fromDb =
            select {
                table "PersonsSimple"
                where (eq "Id" 5)
            } |> conn.SelectAsync<Issues.PersonsSimple.View>
        let row : Issues.PersonsSimple.View = Seq.head fromDb            
        Expect.equal row.Id 5 ""
        Expect.equal row.Desc "UPDATED" ""
    }
    
    testTask "Works with reserved names #8 - LEFT JOIN" {
        do! Issues.PersonsSimple.init conn
        do! Issues.PersonsSimpleDescs.init conn
        let rs = Issues.PersonsSimple.View.generate 10
        let! _ =
            insert {
                table "PersonsSimple"
                values rs
            } |> conn.InsertAsync
        let ds = Issues.PersonsSimpleDescs.View.generate 10            
        let! _ =
            insert {
                table "PersonsSimpleDescs"
                values ds
            } |> conn.InsertAsync
        
        let! fromDb =
            select {
                table "PersonsSimple"
                where (eq "PersonsSimple.Desc" "Desc_5")
                leftJoin "PersonsSimpleDescs" "Desc" "PersonsSimple.Desc"
            } |> conn.SelectAsyncOption<Issues.PersonsSimple.View, Issues.PersonsSimpleDescs.View>
        let (row:Issues.PersonsSimple.View), (desc:Issues.PersonsSimpleDescs.View option) = Seq.head fromDb            
        Expect.equal row.Id 5 ""
        Expect.equal desc.Value.Desc row.Desc ""
    }
    
    testTask "Works with reserved names #8 - INNER JOIN" {
        do! Issues.PersonsSimple.init conn
        do! Issues.PersonsSimpleDescs.init conn
        let rs = Issues.PersonsSimple.View.generate 10
        let! _ =
            insert {
                table "PersonsSimple"
                values rs
            } |> conn.InsertAsync
        let ds = Issues.PersonsSimpleDescs.View.generate 10            
        let! _ =
            insert {
                table "PersonsSimpleDescs"
                values ds
            } |> conn.InsertAsync
        
        let! fromDb =
            select {
                table "PersonsSimple"
                where (eq "PersonsSimple.Desc" "Desc_5")
                innerJoin "PersonsSimpleDescs" "Desc" "PersonsSimple.Desc"
            } |> conn.SelectAsync<Issues.PersonsSimple.View, Issues.PersonsSimpleDescs.View>
        let (row:Issues.PersonsSimple.View), (desc:Issues.PersonsSimpleDescs.View) = Seq.head fromDb            
        Expect.equal row.Id 5 ""
        Expect.equal desc.Desc row.Desc ""
    }
]