module Dapper.FSharp.Tests.MySQL.Database

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
            do! "drop table Persons" |> conn.ExecuteCatchIgnore
            do!
                """
                create table Persons
                (
                    Id char(36) not null,
                    FirstName nvarchar(255) not null,
                    LastName longtext not null,
                    Position int not null,
                    DateOfBirth datetime null
                );

                create unique index Persons_Id_uindex
                    on Persons (Id);
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Dogs =
   
    let init (conn:IDbConnection) =
        task {
            do! "drop table Dogs" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE Dogs (
                    OwnerId char(36) not null,
                    Nickname longtext not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module DogsWeights =

    let init (conn:IDbConnection) =
        task {
            do! "drop table DogsWeights" |> conn.ExecuteCatchIgnore
            do!
                """
                create table DogsWeights (
                DogNickname longtext not null,
                Year smallint not null,
                Weight smallint not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Issues =
    
    module PersonsSimple =
        
        let init (conn:IDbConnection) =
            task {
                do! "drop table PersonsSimple" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table PersonsSimple (
                    Id int not null,
                    Name nvarchar(255) not null,
                    `Desc` nvarchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }
    
    module PersonsSimpleDescs =
        
        let init (conn:IDbConnection) =
            task {
                do! "drop table PersonsSimpleDescs" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table PersonsSimpleDescs (
                    Id int not null,
                    `Desc` nvarchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }
            
    module Group =
        let init (conn:IDbConnection) =
            task {
                do! "drop table `Group`" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table `Group`(
                    Id int not null,
                    Name nvarchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }           