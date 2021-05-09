module Dapper.FSharp.Tests.SelectExpressionTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Expecto
open FSharp.Control.Tasks.V2
open ExpressionBuilders

type Person = {
    Id: int
    FName: string
    MI: string option
    LName: string
    Age: int
}

type Address = {
    PersonId: int
    City: string
    State: string
}

let testsBasic() = testList "SELECT EXPRESSION" [
    
    testTask "Simple Query" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.FName = "John")
                orderBy p.LName
            }

        Expect.equal query.Table "Person" "Expected table = 'Person'"
        Expect.equal query.Where (eq "FName" "John") "Expected FName = 'John'"
        Expect.equal query.OrderBy [("LName", Asc)] "Expected Order By 'LName'"
    }

    testTask "Complex Query" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.FName = "John" && p.LName = "Doe")
                orderByDescending p.LName
                orderByDescending p.Age
            }
    
        Expect.equal query.Table "Person" "Expected table = 'Person'"
        Expect.equal query.Where (eq "FName" "John" + eq "LName" "Doe") "Expected FName = 'John' && LName = 'Doe'"
        Expect.equal query.OrderBy [("LName", Desc); ("Age", Desc)] "Expected Order By 'LName DESC, Age DESC'"
    }

    testTask "Unary Not" {
        let query = 
            select {
                for p in entity<Person> do
                where (not (p.FName = "John"))
            }
    
        Expect.equal query.Where (!!(eq "FName" "John")) "Expected not (FName = 'John')"
    }

    testTask "Group By" {
        let query = 
            select {
                for p in entity<Person> do
                count "*" "Count"
                where (not (p.FName = "John"))
                groupBy p.Age
            }
    
        Expect.equal query.GroupBy ["Age"] "Expected group by 'Age'"
        Expect.equal query.Aggregates [Count ("*", "Count")] "Expected count(*) as [Count]"
    }

    testTask "Optional Column is None" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI = None)
            }
    
        Expect.equal query.Where (isNullValue "MI") "Expected MI is null"
    }

    testTask "Optional Column is not None" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI <> None)
            }
    
        Expect.equal query.Where (isNotNullValue "MI") "Expected MI is not null"
    }

    testTask "Optional Column = Some value" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI = Some "N")
            }
    
        Expect.equal query.Where (eq "MI" "N") "Expected MI = 'N'"
    }

    testTask "SqlMethods.isIn" {
        let query = 
            select {
                for p in entity<Person> do
                where (isIn p.Age [18;21])
            }
    
        Expect.equal query.Where (Column ("Age", In [18;21])) "Expected Age IN (18,21)"
    }

    testTask "SqlMethods.isNotIn" {
        let ages = [1..5]
        let query = 
            select {
                for p in entity<Person> do
                where (isNotIn p.Age ages)
            }
    
        Expect.equal query.Where (Column ("Age", NotIn [1;2;3;4;5])) "Expected Age NOT IN (1,2,3,4,5)"
    }

    testTask "Like" {
        let query = 
            select {
                for p in entity<Person> do
                where (like p.LName "D%")
            }
    
        Expect.equal query.Where (Column ("LName", Like "D%")) "Expected LName LIKE \"D%\""
    }

    testTask "Inner Join" {
        let query = 
            select {
                for p in entity<Person> do
                innerJoin (fun (p: Person) (a: Address) -> p.Id = a.PersonId)
            }
    
        Expect.equal query.Joins [InnerJoin ("Address", "PersonId", "Person.Id")] "Expected INNER JOIN Address ON Person.Id = Address.PersonId"
    }

    testTask "Left Join" {
        let query = 
            select {
                for p in entity<Person> do
                leftJoin (fun (p: Person) (a: Address) -> p.Id = a.PersonId)
            }
    
        Expect.equal query.Joins [LeftJoin ("Address", "PersonId", "Person.Id")] "Expected LEFT JOIN Address ON Person.Id = Address.PersonId"
    }

    testTask "Count" {
        let query = 
            select {
                for p in entity<Person> do
                count "*" "Count"
                where (not (p.FName = "John"))
            }
    
        Expect.equal query.Aggregates [Count ("*", "Count")] "Expected count(*) as [Count]"
    }

    testTask "Max By" {
        let query = 
            select {
                for p in entity<Person> do
                maxBy p.Age
            }
    
        Expect.equal query.Aggregates [Max ("Age", "Age")] "Expected max(Age) as [Age]"
    }
]
