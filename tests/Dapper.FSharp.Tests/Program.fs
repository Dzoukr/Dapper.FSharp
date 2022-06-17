module Dapper.FSharp.Tests.Program

open Dapper.FSharp.Tests.Database
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

let private sharedTests crud init = [
    InsertTests.testsBasic crud init
    DeleteTests.testsBasic crud init
    UpdateTests.testsBasic crud init
    SelectTests.testsBasic crud init
    //TODO
    //IssuesTests.testsBasic crud init
    //IssuesTests.testsOutput crud init
]

let private sharedTestsWithOutputSupport crud init = [
    DeleteTests.testsOutput crud init
    UpdateTests.testsOutput crud init
    InsertTests.testsOutput crud init
]

let mssqlTests connString =
    let conn = new SqlConnection(connString)
    conn |> Dapper.FSharp.Tests.MSSQL.Database.init
    let crud = MSSQL.Database.getCrud conn
    let init = MSSQL.Database.getInitializer conn
    sharedTests crud init
    @ sharedTestsWithOutputSupport crud init
    @ [
        MSSQL.AggregatesTests.tests conn
    ]
    |> testList "MSSQL"
    |> testSequenced

let mysqlTests connString =
    let conn = new MySqlConnection(connString)
    conn |> Dapper.FSharp.Tests.MySQL.Database.init
    let crud = MySQL.Database.getCrud conn
    let init = MySQL.Database.getInitializer conn
    sharedTests crud init
    @ [
        MySQL.AggregatesTests.tests conn
    ]
    |> testList "MySQL"
    |> testSequenced

let postgresTests connString =
    let conn = new NpgsqlConnection(connString)
    conn |> Dapper.FSharp.Tests.PostgreSQL.Database.init
    let crud = PostgreSQL.Database.getCrud conn
    let init = PostgreSQL.Database.getInitializer conn
    sharedTests crud init
    @ sharedTestsWithOutputSupport crud init
    @ [
        PostgreSQL.AggregatesTests.tests conn
    ]
    |> testList "PostgreSQL"
    |> testSequenced

[<EntryPoint>]
let main argv =

    let conf = (ConfigurationBuilder()).AddJsonFile("settings.json").Build()
    Dapper.FSharp.OptionTypes.register()
    [
//        conf.["mssqlConnectionString"] |> mssqlTests
//        conf.["mysqlConnectionString"] |> mysqlTests
//        conf.["postgresConnectionString"] |> postgresTests
        SelectQueryBuilderTests.tests
    ]
    |> testList "✔"
    |> runTests testConfig
