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
    let conn = new SqlConnection(connectionString)
    
    [
        MSSQL.InsertTests.tests conn
        MSSQL.UpdateTests.tests conn
        MSSQL.DeleteTests.tests conn
        MSSQL.SelectTests.tests conn
    ]
    |> Tests.testList "MSSQL" 
    |> Tests.testSequenced
    |> Tests.runTests testConfig