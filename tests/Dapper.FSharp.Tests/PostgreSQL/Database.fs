module Dapper.FSharp.Tests.PostgreSQL.Database

open System
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.Extensions
open System.Data
open Microsoft.Extensions.Configuration
open Npgsql

let getConnection () =
    let conf = ConfigurationBuilder().AddJsonFile("settings.json").Build()
    new NpgsqlConnection(conf.["postgresConnectionString"]) :> IDbConnection

let mutable isAlreadyInitialized = false

let safeInit (conn:IDbConnection) =
    task {
        if isAlreadyInitialized |> not then
            let dbName = DbName.ToLowerInvariant()
            do! dbName |> sprintf "drop database if exists %s;" |> conn.ExecuteCatchIgnore
            do! dbName |> sprintf "create database %s;" |> conn.ExecuteIgnore
            conn.Open()
            conn.ChangeDatabase dbName
            do! TestSchema |> sprintf "drop schema if exists %s;" |> conn.ExecuteIgnore
            do! TestSchema |> sprintf "create schema %s;" |> conn.ExecuteIgnore
            isAlreadyInitialized <- true
            Dapper.FSharp.PostgreSQL.OptionTypes.register()
    }
    |> Async.AwaitTask
    |> Async.RunSynchronously

module Persons =

    let init (conn:IDbConnection) =
        task {
            do! "drop table if exists \"Persons\"" |> conn.ExecuteCatchIgnore
            do!
                """
                create table "Persons"
                (
                    "Id" uuid not null
                        constraint persons_pk
                            primary key,
                    "FirstName" varchar(255) not null,
                    "LastName" text not null,
                    "Position" int not null,
                    "DateOfBirth" date
                );
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Articles =

    let init (conn:IDbConnection) =
        task {
            do! "drop table if exists \"Articles\"" |> conn.ExecuteCatchIgnore
            do!
                """
                create table "Articles"
                (
                    "Id" serial not null
                        constraint table_name_pk
                        primary key,
                    "Title" varchar(255) not null
                )
                """
                |> conn.ExecuteIgnore
        }

module Dogs =

    let init (conn:IDbConnection) =
        task {
            do! "drop table if exists \"Dogs\"" |> conn.ExecuteCatchIgnore
            do!
                """
                create table "Dogs" (
                    "Id" uuid not null,
                    "OwnerId" uuid not null,
                    "Nickname" text not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Vaccinations =

    let init (conn:IDbConnection) =
        task {
            do! "drop table if exists \"Vaccinations\"" |> conn.ExecuteCatchIgnore
            do!
                """
                create table "Vaccinations" (
                    "PetOwnerId" uuid not null,
                    "DogNickname" text not null,
                    "Vaccination" text not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }
        
module VaccinationManufacturers =

    let init (conn:IDbConnection) =
        task {
            do! "drop table if exists \"VaccinationManufacturers\"" |> conn.ExecuteCatchIgnore
            do!
                """
                create table "VaccinationManufacturers" (
                    "Vaccination" text not null,
                    "Manufacturer" text not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }
        
module DogsWeights =

    let init (conn:IDbConnection) =
        task {
            do! "drop table if exists \"DogsWeights\"" |> conn.ExecuteCatchIgnore
            do!
                """
                create table "DogsWeights" (
                "DogNickname" text not null,
                "Year" smallint not null,
                "Weight" smallint not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Issues =

    module PersonsSimple =

        let init (conn:IDbConnection) =
            task {
                do! "drop table if exists \"PersonsSimple\"" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table "PersonsSimple" (
                    "Id" int not null,
                    "Name" varchar(255) not null,
                    "Desc" varchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module PersonsSimpleDescs =

        let init (conn:IDbConnection) =
            task {
                do! "drop table if exists \"PersonsSimpleDescs\"" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table "PersonsSimpleDescs" (
                    "Id" int not null,
                    "Desc" varchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module Group =
        let init (conn:IDbConnection) =
            task {
                do! "drop table if exists \"Group\"" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table "Group"(
                    "Id" int not null,
                    "Name" varchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module SchemedGroup =
        let init (conn:IDbConnection) =
            task {
                do! (sprintf "drop table if exists \"%s\".\"SchemedGroup\"" TestSchema) |> conn.ExecuteCatchIgnore
                do!
                    sprintf """
                    create table "%s"."SchemedGroup"(
                    "Id" int not null,
                    "SchemedName" varchar(255) not null
                    )
                    """ TestSchema
                    |> conn.ExecuteIgnore
                return ()
            }

open Dapper.FSharp.PostgreSQL

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