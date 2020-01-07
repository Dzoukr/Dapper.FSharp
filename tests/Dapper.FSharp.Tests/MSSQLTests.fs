module Dapper.FSharp.Tests.MSSQLTests

open System
open System.Data
open Expecto
open Dapper
open FSharp.Control.Tasks.V2

let withTable tn (str:string) = str.Replace("<TABLE_NAME>", tn)

type System.Data.IDbConnection with
    member this.ExecuteIgnore (cmd:string) =
        task {
            let! _ = this.ExecuteAsync(cmd)
            return ()
        }
    member this.ExecuteCatchIgnore (cmd:string) =
        task {
            try
                do! this.ExecuteIgnore(cmd)
            with _ -> return ()
        }
            

module Persons =
    
    type View = {
        Id : Guid
        FirstName : string
        LastName : string
        Position : int
        DateOfBirth : DateTime option
    }
    
    type ViewRequired = {
        Id : Guid
        FirstName : string
        LastName : string
        Position : int
    }
    
    module View =
        let generate x =
            [1..x]
            |> List.map (fun x ->
                {
                    Id = Guid.NewGuid()
                    FirstName = sprintf "First_%i" x
                    LastName = sprintf "Last_%i" x
                    DateOfBirth = if x%2=0 then None else Some (x |> float |> DateTime.Today.AddDays)
                    Position = x
                }
            ) 
    
    let tableName = "Persons"
    
    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE <TABLE_NAME>" |> withTable tableName |> conn.ExecuteCatchIgnore
            do! 
                """
                CREATE TABLE [dbo].[<TABLE_NAME>](
                    [Id] [uniqueidentifier] NOT NULL,
                    [FirstName] [nvarchar](max) NOT NULL,
                    [LastName] [nvarchar](max) NOT NULL,
                    [Position] [int] NOT NULL,
                    [DateOfBirth] [datetime] NULL,
                 CONSTRAINT [PK_<TABLE_NAME>] PRIMARY KEY CLUSTERED 
                (
                    [Id] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
                """
                |> withTable tableName
                |> conn.ExecuteIgnore
            return ()                
        }

open Dapper.FSharp
open Dapper.FSharp.MSSQL
       
let private insertTests (conn:IDbConnection) = [
    
    testTask "Inserts new record" {
        do! Persons.init conn
        let r = Persons.View.generate 1 |> List.head
        let! _ =
            insert {
                table Persons.tableName
                value r
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table Persons.tableName
                where (column "Id" (Eq r.Id))
            } |> conn.SelectAsync<Persons.View>
        Expect.equal r (Seq.head fromDb) ""                            
    }
    
    testTask "Inserts partial record" {
        do! Persons.init conn
        let r =
            Persons.View.generate 1
            |> List.head
            |> fun x -> ({ Id = x.Id; FirstName = x.FirstName; LastName = x.LastName; Position = x.Position } : Persons.ViewRequired)
        let! _ =
            insert {
                table Persons.tableName
                value r
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table Persons.tableName
                where (column "Id" (Eq r.Id))
            } |> conn.SelectAsync<Persons.ViewRequired>
        Expect.equal r (Seq.head fromDb) ""                            
    }
    
    testTask "Inserts more records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! fromDb =
            select {
                table Persons.tableName
                orderBy "Position" Asc
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal rs (Seq.toList fromDb) ""
    }
]

let private updateTests (conn:IDbConnection) = [
    
    testTask "Updates single records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table Persons.tableName
                set {| LastName = "UPDATED" |}
                where (column "Position" (Eq 2))
            } |> conn.UpdateAsync
        let! fromDb =
            select {
                table Persons.tableName
                where (column "LastName" (Eq "UPDATED"))
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 2 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
    
    testTask "Updates more records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            update {
                table Persons.tableName
                set {| LastName = "UPDATED" |}
                where (column "Position" (Gt 7))
            } |> conn.UpdateAsync
        
        let! fromDb =
            select {
                table Persons.tableName
                where (column "LastName" (Eq "UPDATED"))
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal 3 (Seq.length fromDb) ""
    }
]

let private deleteTests (conn:IDbConnection) = [
    
    testTask "Deletes single records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                table Persons.tableName
                where (column "Position" (Eq 10))
            } |> conn.DeleteAsync
        let! fromDb =
            select {
                table Persons.tableName
                orderBy "Position" Desc
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal 9 (Seq.length fromDb) ""
        Expect.equal 9 (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
    
    testTask "Deletes more records" {
        do! Persons.init conn
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table Persons.tableName
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                table Persons.tableName
                where (column "Position" (Ge 7))
            } |> conn.DeleteAsync
        
        let! fromDb =
            select {
                table Persons.tableName
            } |> conn.SelectAsync<Persons.View>            
        Expect.equal 6 (Seq.length fromDb) ""
    }
]
    
let tests conn = [
    Tests.testList "INSERT" (insertTests conn)
    Tests.testList "UPDATE" (updateTests conn)
    Tests.testList "DELETE" (deleteTests conn)
]