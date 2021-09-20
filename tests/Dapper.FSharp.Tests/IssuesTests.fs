module Dapper.FSharp.Tests.IssuesTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto
open FSharp.Control.Tasks.V2
//
//let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "Issues" [
//    
//    testTask "Works with reserved names #8" {
//        do! init.InitPersonsSimple()
//        let rs = Issues.PersonsSimple.View.generate 10
//        let! _ =
//            insert {
//                table "PersonsSimple"
//                values rs
//            } |> crud.InsertAsync
//        let! _ =
//            update {
//                table "PersonsSimple"
//                set {| Desc = "UPDATED" |}
//                where (eq "Id" 5)
//            } |> crud.UpdateAsync
//        let! fromDb =
//            select {
//                table "PersonsSimple"
//                where (eq "Id" 5)
//            } |> crud.SelectAsync<Issues.PersonsSimple.View>
//        let row : Issues.PersonsSimple.View = Seq.head fromDb            
//        Expect.equal row.Id 5 ""
//        Expect.equal row.Desc "UPDATED" ""
//    }
//    
//    testTask "Works with reserved names #8 - LEFT JOIN" {
//        do! init.InitPersonsSimple()
//        do! init.InitPersonsSimpleDescs()
//        let rs = Issues.PersonsSimple.View.generate 10
//        let! _ =
//            insert {
//                table "PersonsSimple"
//                values rs
//            } |> crud.InsertAsync
//        let ds = Issues.PersonsSimpleDescs.View.generate 10            
//        let! _ =
//            insert {
//                table "PersonsSimpleDescs"
//                values ds
//            } |> crud.InsertAsync
//        
//        let! fromDb =
//            select {
//                table "PersonsSimple"
//                where (eq "PersonsSimple.Desc" "Desc_5")
//                leftJoin "PersonsSimpleDescs" "Desc" "PersonsSimple.Desc"
//            } |> crud.SelectAsyncOption<Issues.PersonsSimple.View, Issues.PersonsSimpleDescs.View>
//        let (row:Issues.PersonsSimple.View), (desc:Issues.PersonsSimpleDescs.View option) = Seq.head fromDb            
//        Expect.equal row.Id 5 ""
//        Expect.equal desc.Value.Desc row.Desc ""
//    }
//    
//    testTask "Works with reserved names #8 - INNER JOIN" {
//        do! init.InitPersonsSimple()
//        do! init.InitPersonsSimpleDescs()
//        let rs = Issues.PersonsSimple.View.generate 10
//        let! _ =
//            insert {
//                table "PersonsSimple"
//                values rs
//            } |> crud.InsertAsync
//        let ds = Issues.PersonsSimpleDescs.View.generate 10            
//        let! _ =
//            insert {
//                table "PersonsSimpleDescs"
//                values ds
//            } |> crud.InsertAsync
//        
//        let! fromDb =
//            select {
//                table "PersonsSimple"
//                where (eq "PersonsSimple.Desc" "Desc_5")
//                innerJoin "PersonsSimpleDescs" "Desc" "PersonsSimple.Desc"
//            } |> crud.SelectAsync<Issues.PersonsSimple.View, Issues.PersonsSimpleDescs.View>
//        let (row:Issues.PersonsSimple.View), (desc:Issues.PersonsSimpleDescs.View) = Seq.head fromDb            
//        Expect.equal row.Id 5 ""
//        Expect.equal desc.Desc row.Desc ""
//    }
//    
//    
//    
//    testTask "Works for tables with reserved names #11" {
//        let getFirst () : Task<Group.View option> =
//            task {
//                let! rows = select { table "Group" } |> crud.SelectAsync<Group.View>
//                return rows |> Seq.tryHead
//            }
//        
//        do! init.InitGroups()
//        let! _ =
//            insert {
//                table "Group"
//                value ({ Id = 1; Name = "My" } : Group.View)
//            } |> crud.InsertAsync
//        let! (row:Group.View option) = getFirst()
//        Expect.equal "My" row.Value.Name ""
//        let! _ =
//            update {
//                table "Group"
//                set {| Name = "Updated" |}
//                where (eq "Id" 1)
//            } |> crud.UpdateAsync
//        let! (row:Group.View option) = getFirst()
//        Expect.equal "Updated" row.Value.Name ""
//        let! _ = delete { table "Group" } |> crud.DeleteAsync
//        let! (row:Group.View option) = getFirst()
//        Expect.isNone row ""
//    }
//    
//    testTask "Works in different schema" {
//        do! init.InitSchemedGroups()
//        let! _ =
//            insert {
//                schema TestSchema
//                table "SchemedGroup"
//                value {| Id = 1; SchemedName = "Hi" |}
//            } |> crud.InsertAsync
//        let! _ =
//            update {
//                schema TestSchema
//                table "SchemedGroup"
//                set {| SchemedName = "UPDATED" |}
//                where (eq "Id" 1)
//            } |> crud.UpdateAsync
//        let res =
//            select {
//                schema TestSchema
//                table "SchemedGroup"
//                where (eq "Id" 1)
//            }
//            |> crud.SelectAsync<{| Id : int; SchemedName : string |}>
//            |> taskToList
//        Expect.equal res.Head.SchemedName "UPDATED" ""
//    }
//]
//
//let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "Issues OUTPUT" [
//    testTask "Returns auto-increment value back" {
//        do! init.InitArticles()
//        let! ins =
//            insert {
//                table "Articles"
//                value ({| Title = "MyTitle" |})
//            } |> crud.InsertOutputAsync<{| Title : string |}, {| Id : int |}>
//        let lastInserted = ins |> Seq.head |> (fun (x:{| Id : int |}) -> x.Id)
//        Expect.equal 1 lastInserted ""
//    }
//]