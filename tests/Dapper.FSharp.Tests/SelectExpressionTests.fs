module Dapper.FSharp.Tests.SelectExpressionTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto
open FSharp.Control.Tasks.V2

type Person = {
    FName: string
    LName: string
}

open ExpressionBuilders

let testsBasic() = testList "SELECT EXPRESSION" [
    
    testTask "Simple Where" {

        let query = 
            select {
                for p in tbl<Person> do                
                where (p.FName = "John")
            }

        Expect.equal (query.Where) (eq "FName" "John") "Expected FName = 'John'"
    }

    testTask "Compound Where" {
    
            let query = 
                select {
                    for p in tbl<Person> do                
                    where (p.FName = "John" && p.LName = "Doe")
                }
    
            Expect.equal (query.Where) (eq "FName" "John" + eq "LName" "Doe") "Expected FName = 'John' && LName = 'Doe'"
        }

]
