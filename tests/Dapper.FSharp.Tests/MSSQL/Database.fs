module Dapper.FSharp.Tests.MSSQL.Database

open Dapper.FSharp.Tests.Extensions
open System
open System.Data
open FSharp.Control.Tasks

let [<Literal>] DbName = "DapperFSharpTests"

let init (conn:IDbConnection) =
    task {
        do! DbName |> sprintf "DROP DATABASE IF EXISTS %s;" |> conn.ExecuteIgnore
        do! DbName |> sprintf "CREATE DATABASE %s;" |> conn.ExecuteIgnore
        conn.Open()
        conn.ChangeDatabase DbName
    } |> Async.AwaitTask |> Async.RunSynchronously

module Persons =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE Persons" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [dbo].[Persons](
                    [Id] [uniqueidentifier] NOT NULL,
                    [FirstName] [nvarchar](max) NOT NULL,
                    [LastName] [nvarchar](max) NOT NULL,
                    [Position] [int] NOT NULL,
                    [DateOfBirth] [datetime] NULL,
                 CONSTRAINT [PK_Persons] PRIMARY KEY CLUSTERED
                (
                    [Id] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Dogs =
   
    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE Dogs" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [dbo].[Dogs](
                    [OwnerId] [uniqueidentifier] NOT NULL,
                    [Nickname] [nvarchar](max) NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module DogsWeights =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE DogsWeights" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [dbo].[DogsWeights](
	            [DogNickname] [nvarchar](max) NOT NULL,
	            [Year] [smallint] NOT NULL,
	            [Weight] [smallint] NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Issues =
    
    module PersonsSimple =
        
        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE PersonsSimple" |> conn.ExecuteCatchIgnore
                do!
                    """
                    CREATE TABLE [dbo].[PersonsSimple](
	                [Id] [int] NOT NULL,
	                [Name] [nvarchar](max) NOT NULL,
	                [Desc] [nvarchar](max) NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }
    
    module PersonsSimpleDescs =
        
        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE PersonsSimpleDescs" |> conn.ExecuteCatchIgnore
                do!
                    """
                    CREATE TABLE [dbo].[PersonsSimpleDescs](
	                [Id] [int] NOT NULL,
	                [Desc] [nvarchar](max) NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }
        