module Dapper.FSharp.Tests.PostgreSQL.Database

open Dapper.FSharp.Tests.Extensions
open System
open System.Data
open FSharp.Control.Tasks

let [<Literal>] DbName = "dapperfsharptests"
let [<Literal>] TestSchema = "tests"

let init (conn:IDbConnection) =
    task {
        do! DbName |> sprintf "drop database if exists %s;" |> conn.ExecuteCatchIgnore
        do! DbName |> sprintf "create database %s;" |> conn.ExecuteIgnore
        conn.Open()
        conn.ChangeDatabase DbName
        do! TestSchema |> sprintf "drop schema if exists %s;" |> conn.ExecuteIgnore
        do! TestSchema |> sprintf "create schema %s;" |> conn.ExecuteIgnore
    } |> Async.AwaitTask |> Async.RunSynchronously

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
                    "OwnerId" uuid not null,
                    "Nickname" text not null
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