module Dapper.FSharp.Tests.Program

open Expecto
open Expecto.Logging
open Microsoft.Data.SqlClient
open Microsoft.Extensions.Configuration
open MySql.Data.MySqlClient
open Npgsql

let testConfig = 
    { defaultConfig with 
        parallelWorkers = 4
        verbosity = LogLevel.Debug }

let selectExprTests = 
    [SelectExpressionTests.testsBasic()]
    |> testList "EXPR"
    |> testSequenced

let mssqlTests connString =
    let conn = new SqlConnection(connString)
    conn |> Dapper.FSharp.Tests.MSSQL.Database.init
    let crud = MSSQL.Database.getCrud conn
    let init = MSSQL.Database.getInitializer conn
    [
        DeleteTests.testsBasic crud init
        DeleteTests.testsOutput crud init
        InsertTests.testsBasic crud init
        InsertTests.testsOutput crud init
        IssuesTests.testsBasic crud init
        IssuesTests.testsOutput crud init
        UpdateTests.testsBasic crud init
        UpdateTests.testsOutput crud init
        SelectTests.testsBasic crud init        
        MSSQL.AggregatesTests.tests conn
    ]
    |> testList "MSSQL"
    |> testSequenced

let mysqlTests connString =
    let conn = new MySqlConnection(connString)
    conn |> Dapper.FSharp.Tests.MySQL.Database.init
    let crud = MySQL.Database.getCrud conn
    let init = MySQL.Database.getInitializer conn
    [
        DeleteTests.testsBasic crud init
        InsertTests.testsBasic crud init
        IssuesTests.testsBasic crud init
        UpdateTests.testsBasic crud init
        SelectTests.testsBasic crud init
        MySQL.AggregatesTests.tests conn
    ]
    |> testList "MySQL"
    |> testSequenced

let postgresTests connString =
    let conn = new NpgsqlConnection(connString)
    conn |> Dapper.FSharp.Tests.PostgreSQL.Database.init
    let crud = PostgreSQL.Database.getCrud conn
    let init = PostgreSQL.Database.getInitializer conn
    [
        DeleteTests.testsBasic crud init
        DeleteTests.testsOutput crud init
        InsertTests.testsBasic crud init
        InsertTests.testsOutput crud init
        IssuesTests.testsBasic crud init
        IssuesTests.testsOutput crud init
        UpdateTests.testsBasic crud init
        UpdateTests.testsOutput crud init
        SelectTests.testsBasic crud init
        PostgreSQL.AggregatesTests.tests conn
    ]
    |> testList "PostgreSQL"
    |> testSequenced

[<EntryPoint>]
let main argv =
    match argv with
    | [| "EXPR" |] -> 
        // Only run expression unit tests
        selectExprTests
        |> runTests testConfig
    | _ -> 
        // Run all tests
        let conf = (ConfigurationBuilder()).AddJsonFile("local.settings.json").Build()
        Dapper.FSharp.OptionTypes.register()
        [
            conf.["mssqlConnectionString"] |> mssqlTests
            conf.["mysqlConnectionString"] |> mysqlTests
            conf.["postgresConnectionString"] |> postgresTests
            selectExprTests
        ]
        |> testList ""
        |> runTests testConfig
