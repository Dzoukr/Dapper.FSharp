module Dapper.FSharp.Tests.MSSQL.Database

open Dapper.FSharp.Tests.Extensions
open System
open System.Data
open FSharp.Control.Tasks

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

    type View = {
        DogNickname : string
        Year : int16
        Weight : int16
    }

    module View =
        let generate1to1 (dogs:Dogs.View list) =
            dogs
            |> List.mapi (fun i x ->
                {
                    DogNickname = x.Nickname
                    Year = 2010s + (int16 i)
                    Weight = 10s + (int16 i)
                }
            )

        let generate1toN count (dog:Dogs.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    DogNickname = dog.Nickname
                    Year = 2010s + (int16 i)
                    Weight = 10s + (int16 i)
                }

            )

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
