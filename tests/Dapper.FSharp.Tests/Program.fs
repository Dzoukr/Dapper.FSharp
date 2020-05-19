module Dapper.FSharp.Tests.Program

open System
open Expecto
open Expecto.Logging
open Microsoft.Data.SqlClient
open Microsoft.Extensions.Configuration
open MySql.Data.MySqlClient

let testConfig = 
    { Expecto.Tests.defaultConfig with 
        parallelWorkers = 4
        verbosity = LogLevel.Debug }

let mssqlTests connString =
    let mssql = new SqlConnection(connString)
    mssql |> Dapper.FSharp.Tests.MSSQL.Database.init
    [
        MSSQL.InsertTests.tests mssql
        MSSQL.UpdateTests.tests mssql
        MSSQL.DeleteTests.tests mssql
        MSSQL.SelectTests.tests mssql
    ]
    |> Tests.testList "MSSQL"
    |> Tests.testSequenced

let mysqlTests connString =
    let mysql = new MySqlConnection(connString)
    mysql |> Dapper.FSharp.Tests.MySQL.Database.init
    [
        MySQL.InsertTests.tests mysql
        MySQL.UpdateTests.tests mysql
        MySQL.DeleteTests.tests mysql
        MySQL.SelectTests.tests mysql
    ]
    |> Tests.testList "MySQL"
    |> Tests.testSequenced

[<EntryPoint>]
let main _ =
    let conf = (ConfigurationBuilder()).AddJsonFile("local.settings.json").Build()
    
    Dapper.FSharp.OptionTypes.register()
    
    [
        conf.["mssqlConnectionString"] |> mssqlTests
        conf.["mysqlConnectionString"] |> mysqlTests
    ]
    |> Tests.testList ""
    |> Tests.runTests testConfig