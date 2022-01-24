module Dapper.FSharp.Tests.MSSQL.Database

open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.Extensions
open System.Data
open FSharp.Control.Tasks

let init (conn:IDbConnection) =
    task {
        do! DbName |> sprintf "DROP DATABASE IF EXISTS %s;" |> conn.ExecuteIgnore
        do! DbName |> sprintf "CREATE DATABASE %s;" |> conn.ExecuteIgnore
        conn.Open()
        conn.ChangeDatabase DbName
        do! TestSchema |> sprintf "DROP SCHEMA IF EXISTS %s;" |> conn.ExecuteIgnore
        do! TestSchema |> sprintf "CREATE SCHEMA %s;" |> conn.ExecuteIgnore
    } |> Async.AwaitTask |> Async.RunSynchronously

module Persons =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE [dbo].Persons" |> conn.ExecuteCatchIgnore
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

module Articles =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE Articles" |> conn.ExecuteCatchIgnore
            do!
                """
                create table Articles
                (
                    Id int identity
                        constraint Articles_pk
                            primary key nonclustered,
                    Title nvarchar(255) not null
                )
                """
                |> conn.ExecuteIgnore
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

module VaccinationHistory =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE [dbo].VaccinationHistory" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [dbo].[VaccinationHistory] (
                    [PetOwnerId] [uniqueidentifier] NOT NULL,
                    [DogNickname] [nvarchar](max) NOT NULL,
                    [VaccinationDate] datetime2 NOT NULL
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

    module Group =
        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE Group" |> conn.ExecuteCatchIgnore
                do!
                    """
                    CREATE TABLE [dbo].[Group](
                    [Id] [int] NOT NULL,
                    [Name] [nvarchar](max) NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module SchemedGroup =
        let init (conn:IDbConnection) =
            task {
                do! (sprintf "DROP TABLE [%s].SchemedGroup" TestSchema) |> conn.ExecuteCatchIgnore
                do!
                    sprintf """
                    CREATE TABLE [%s].[SchemedGroup](
                    [Id] [int] NOT NULL,
                    [SchemedName] [nvarchar](max) NOT NULL
                    )
                    """ TestSchema
                    |> conn.ExecuteIgnore
                return ()
            }

open Dapper.FSharp.MSSQL

let getCrud (conn:IDbConnection) =
    { new ICrudOutput with
        member x.SelectAsync<'a> (q, cancellationToken) = conn.SelectAsync<'a>(q, ?cancellationToken = cancellationToken)
        member x.SelectAsync<'a,'b> q = conn.SelectAsync<'a,'b>(q)
        member x.SelectAsync<'a,'b,'c> q = conn.SelectAsync<'a,'b,'c>(q)
        member x.SelectAsyncOption<'a,'b> q = conn.SelectAsyncOption<'a,'b>(q)
        member x.SelectAsyncOption<'a,'b,'c> q = conn.SelectAsyncOption<'a,'b,'c>(q)
        member x.InsertAsync<'a> (q, cancellationToken) = conn.InsertAsync<'a>(q, ?cancellationToken = cancellationToken)
        member x.DeleteAsync (q, cancellationToken) = conn.DeleteAsync(q, ?cancellationToken = cancellationToken)
        member x.UpdateAsync (q, cancellationToken) = conn.UpdateAsync(q, ?cancellationToken = cancellationToken)
        member x.InsertOutputAsync q = conn.InsertOutputAsync(q)
        member x.DeleteOutputAsync q = conn.DeleteOutputAsync(q)
        member x.UpdateOutputAsync q = conn.UpdateOutputAsync(q)
    }

let getInitializer (conn:IDbConnection) =
    { new ICrudInitializer with
        member x.InitPersons () = Persons.init conn
        member x.InitPersonsSimple () = Issues.PersonsSimple.init conn
        member x.InitPersonsSimpleDescs () = Issues.PersonsSimpleDescs.init conn
        member x.InitArticles () = Articles.init conn
        member x.InitGroups () = Issues.Group.init conn
        member x.InitSchemedGroups () = Issues.SchemedGroup.init conn
        member x.InitDogs () = Dogs.init conn
        member x.InitDogsWeights () = DogsWeights.init conn
        member x.InitVaccinationHistory () = VaccinationHistory.init conn
    }