module Dapper.FSharp.Tests.SQLite.Database

open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.Extensions
open System.Data
open Microsoft.Data.Sqlite
open Microsoft.Extensions.Configuration

let getConnection () =
    let conf = ConfigurationBuilder().AddJsonFile("settings.json").Build()
    new SqliteConnection(conf.["sqliteConnectionString"])

let mutable isAlreadyInitialized = false

let safeInit (conn:IDbConnection) =
    task {
        if isAlreadyInitialized |> not then
            
            conn.Open()
            isAlreadyInitialized <- true
            Dapper.FSharp.SQLite.OptionTypes.register()
    }
    |> Async.AwaitTask
    |> Async.RunSynchronously

module Persons =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE IF EXISTS Persons" |> conn.ExecuteIgnore
            do!
                """
                CREATE TABLE [Persons](
                    [Id] [TEXT] NOT NULL PRIMARY KEY,
                    [FirstName] [TEXT] NOT NULL,
                    [LastName] [TEXT] NOT NULL,
                    [Position] [INTEGER] NOT NULL,
                    [DateOfBirth] [TEXT] NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Articles =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE IF EXISTS Articles" |> conn.ExecuteIgnore
            do!
                """
                create table Articles
                (
                    Id int identity
                        constraint Articles_pk
                            primary key,
                    Title TEXT not null
                )
                """
                |> conn.ExecuteIgnore
        }

module Dogs =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE IF EXISTS Dogs" |> conn.ExecuteIgnore
            do!
                """
                CREATE TABLE [Dogs](
                    [Id] [TEXT] NOT NULL,
                    [OwnerId] [TEXT] NOT NULL,
                    [Nickname] [TEXT] NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Vaccinations =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE IF EXISTS Vaccinations" |> conn.ExecuteIgnore
            do!
                """
                CREATE TABLE [Vaccinations] (
                    [PetOwnerId] [TEXT] NOT NULL,
                    [DogNickname] [TEXT] NOT NULL,
                    [Vaccination] TEXT NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }
        
module VaccinationManufacturers =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE IF EXISTS VaccinationManufacturers" |> conn.ExecuteIgnore
            do!
                """
                CREATE TABLE [VaccinationManufacturers] (
                    [Vaccination] TEXT NOT NULL,
                    [Manufacturer] TEXT NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }
        
module DogsWeights =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE IF EXISTS DogsWeights" |> conn.ExecuteIgnore
            do!
                """
                CREATE TABLE [DogsWeights](
                [DogNickname] [TEXT] NOT NULL,
                [Year] [INTEGER] NOT NULL,
                [Weight] [INTEGER] NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Issues =

    module PersonsSimple =

        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE IF EXISTS PersonsSimple" |> conn.ExecuteIgnore
                do!
                    """
                    CREATE TABLE [PersonsSimple](
                    [Id] [INTEGER] NOT NULL,
                    [Name] [TEXT] NOT NULL,
                    [Desc] [TEXT] NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module PersonsSimpleDescs =

        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE IF EXISTS PersonsSimpleDescs" |> conn.ExecuteIgnore
                do!
                    """
                    CREATE TABLE [PersonsSimpleDescs](
                    [Id] [INTEGER] NOT NULL,
                    [Desc] [TEXT] NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module Group =
        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE IF EXISTS Group" |> conn.ExecuteIgnore
                do!
                    """
                    CREATE TABLE [Group](
                    [Id] [INTEGER] NOT NULL,
                    [Name] [TEXT] NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module SchemedGroup =
        let init (conn:IDbConnection) =
            task {
                do! (sprintf "DROP TABLE IF EXISTS SchemedGroup") |> conn.ExecuteIgnore
                do!
                    sprintf """
                    CREATE TABLE [SchemedGroup](
                    [Id] [INTEGER] NOT NULL,
                    [SchemedName] [TEXT] NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
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
        member x.InitVaccinations () = Vaccinations.init conn
        member x.InitVaccinationManufacturers () = VaccinationManufacturers.init conn
    }