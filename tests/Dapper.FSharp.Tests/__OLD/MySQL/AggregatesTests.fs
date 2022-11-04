module Dapper.FSharp.Tests.MySQL.AggregatesTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.MySQL.Database
open Dapper.FSharp
open Dapper.FSharp.MySQL

let persons = table'<Persons.View> "Persons"
let dogs = table'<Dogs.View> "Dogs"

let tests (conn:IDbConnection) = Tests.testList "SELECT - AGGREGATES" [
    
]