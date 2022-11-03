module Dapper.FSharp.Tests.SelectTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto
open System.Threading
open Dapper.FSharp.Tests.Extensions

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "SELECT" [

    let personsView = table'<Persons.View> "Persons" // |> inSchema "dbo"
    let dogsView = table'<Dogs.View> "Dogs" //|> inSchema "dbo"
    let dogsWeightsView = table'<DogsWeights.View> "DogsWeights" // |> inSchema "dbo"
    
    testTask 
    
    testTask 

    testTask "

    testTask "

    testTask "

    testTask "

    testTask "

    testTask "
    
    testTask "

    testTask "

    testTask "

    testTask "

    testTask "

    testTask "

    testTask "
]