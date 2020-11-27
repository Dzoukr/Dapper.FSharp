module Dapper.FSharp.Tests.MSSQL.IssuesTests

open System.Data
open System.Threading.Tasks
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.MSSQL.Database
open Dapper.FSharp
open Dapper.FSharp.MSSQL
open FSharp.Control.Tasks.V2

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
    
    testTask "Returns auto-increment value back" {
        do! Articles.init conn
        let! ins =
            insert {
                table "Articles"
                value ({| Title = "MyTitle" |})
            } |> conn.InsertOutputAsync<{| Title : string |}, {| Id : int |}>
        let lastInserted = ins |> Seq.head |> (fun (x:{| Id : int |}) -> x.Id)
        Expect.equal 1 lastInserted ""
    }
    
    testTask "Works for tables with reserved names #11" {
        let getFirst () : Task<Group.View option> =
            task {
                let! rows = select { table "Group" } |> conn.SelectAsync<Group.View>
                return rows |> Seq.tryHead
            }
        
        do! Issues.Group.init conn
        let! _ =
            insert {
                table "Group"
                value ({ Id = 1; Name = "My" } : Group.View)
            } |> conn.InsertAsync
        let! (row:Group.View option) = getFirst()
        Expect.equal "My" row.Value.Name ""
        let! _ =
            update {
                table "Group"
                set {| Name = "Updated" |}
                where (eq "Id" 1)
            } |> conn.UpdateAsync
        let! (row:Group.View option) = getFirst()
        Expect.equal "Updated" row.Value.Name ""
        let! _ = delete { table "Group" } |> conn.DeleteAsync
        let! (row:Group.View option) = getFirst()
        Expect.isNone row ""
    }
    
    testTask "Works in different schema" {
        do! Issues.SchemedGroup.init conn
        let! _ =
            insert {
                schema TestSchema
                table "SchemedGroup"
                value {| Id = 1; SchemedName = "Hi" |}
            } |> conn.InsertAsync
        let! _ =
            update {
                schema TestSchema
                table "SchemedGroup"
                set {| SchemedName = "UPDATED" |}
                where (eq "Id" 1)
            } |> conn.UpdateAsync
        let res =
            select {
                schema TestSchema
                table "SchemedGroup"
                where (eq "Id" 1)
            }
            |> conn.SelectAsync<{| Id : int; SchemedName : string |}>
            |> taskToList
        Expect.equal res.Head.SchemedName "UPDATED" ""
    }
]