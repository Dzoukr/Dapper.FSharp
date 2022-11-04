module Dapper.FSharp.Tests.PostgreSQL.AggregatesTests

open System.Data
open Expecto
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Tests.PostgreSQL.Database
open Dapper.FSharp.PostgreSQL

let persons = table'<Persons.View> "Persons"
let dogs = table'<Dogs.View> "Dogs"

let tests (conn:IDbConnection) = Tests.testList "SELECT - AGGREGATES" [
    
]