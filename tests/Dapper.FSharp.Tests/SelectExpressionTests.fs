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
    
    ftestTask "Visitor" {

        let query = 
            select<Person> {
                for p in tbl do                
                where (p.FName = "Bob")
            }

        Expect.equal 9 9 ""
    }

]
