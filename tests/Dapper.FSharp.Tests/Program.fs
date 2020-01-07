module Dapper.FSharp.Tests.Program

open System
open Expecto
open Expecto.Logging
open Microsoft.Data.SqlClient
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Configuration.Json

let testConfig = 
    { Expecto.Tests.defaultConfig with 
        parallelWorkers = 1
        verbosity = LogLevel.Debug }

[<EntryPoint>]
let main _ =
    let conf = (ConfigurationBuilder()).AddJsonFile("local.settings.json").Build()
    let connectionString = conf.["connectionString"]                      
    
    Dapper.FSharp.OptionTypes.register()
    let connection = new SqlConnection(connectionString)
    
    Tests.testList "MSSQL" (MSSQLTests.tests connection)
    |> Tests.testSequenced
    |> Tests.runTests testConfig