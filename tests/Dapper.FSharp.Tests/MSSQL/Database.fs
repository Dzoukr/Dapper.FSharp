module Dapper.FSharp.Tests.MSSQL.Database

open Dapper.FSharp.Tests.Extensions
open System
open System.Data
open FSharp.Control.Tasks

let private withTable tn (str:string) = str.Replace("<TABLE_NAME>", tn)

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
        
module Dogs =
    
    type View = {
        OwnerId : Guid
        Nickname : string
    }
    
    module View =
        let generate1to1 (owners:Persons.View list) =
            owners
            |> List.mapi (fun i x ->
                {
                    OwnerId = x.Id
                    Nickname = sprintf "Dog_%i" i
                }
            )
        
        let generate1toN count (owner:Persons.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    OwnerId = owner.Id
                    Nickname = sprintf "Dog_%i" i
                }
            )
        
    
    let tableName = "Dogs"
    
    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE <TABLE_NAME>" |> withTable tableName |> conn.ExecuteCatchIgnore
            do! 
                """
                CREATE TABLE [dbo].[<TABLE_NAME>](
                    [OwnerId] [uniqueidentifier] NOT NULL,
                    [Nickname] [nvarchar](max) NOT NULL
                )
                """
                |> withTable tableName
                |> conn.ExecuteIgnore
            return ()                
        }        