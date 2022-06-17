module Dapper.FSharp.Tests.SelectQueryBuilderTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.Builders
open Dapper.FSharp.Builders.Operators
open Expecto
open FSharp.Control.Tasks.V2

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

type Contact = {
    PersonId: int
    Phone: string
}

type Vehicle = {
    Id: System.Guid
    Make: string
    Model: string
}

type MultiJoinLeft = {
    Key1: int
    Key2: int64
    LeftName: string
}

type MultiJoinRight = {
    Key1: int
    Key2: int64
    RightName: string
}

/// Creates WHERE condition for column
let column name whereComp = Where.Column(name, whereComp)
/// WHERE column value equals to
let eq name (o:obj) = column name (Eq o)
/// WHERE column value not equals to
let ne name (o:obj) = column name (Ne o)
/// WHERE column value greater than
let gt name (o:obj) = column name (Gt o)
/// WHERE column value lower than
let lt name (o:obj) = column name (Lt o)
/// WHERE column value greater/equals than
let ge name (o:obj) = column name (Ge o)
/// WHERE column value lower/equals than
let le name (o:obj) = column name (Le o)
/// WHERE column like value
let like name (str:string) = column name (Like str)
/// WHERE column not like value
let notLike name (str:string) = column name (NotLike str)
/// WHERE column is IN values
let isIn name (os:obj list) = column name (In os)
/// WHERE column is NOT IN values
let isNotIn name (os:obj list) = column name (NotIn os)
/// WHERE column IS NULL
let isNullValue name = column name IsNull
/// WHERE column IS NOT NULL
let isNotNullValue name = column name IsNotNull

let tests = testList "SELECT QUERY BUILDER" [
    
    testTask "Most Simple Query" {
        let query = 
            select {
                for p in table<Person> do
                selectAll
            }

        Expect.equal query.Table "Person" "Expected table = 'Person'"
    }

    testTask "Simple Query" {
        let query = 
            select {
                for p in table<Person> do
                where (p.FName = "John")
                orderBy p.LName
            }

        Expect.equal query.Table "Person" "Expected table = 'Person'"
        Expect.equal query.Where (eq "Person.FName" "John") "Expected WHERE Person.FName = 'John'"
        Expect.equal query.OrderBy [("Person.LName", Asc)] "Expected ORDER BY Person.LName"
    }

    testTask "Col = Col Where" {
        let query = 
            select {
                for p in table<Person> do
                where (p.FName = p.LName)
            }
        
        Expect.equal query.Where (Expr "Person.FName = Person.LName") "Expected WHERE Person.FName = Person.LName"
    }

    testTask "Col option = Col option Where" {
        let query = 
            select {
                for p in table<Person> do
                where (p.MI = p.MI)
            }
        
        Expect.equal query.Where (Expr "Person.MI = Person.MI") "Expected WHERE Person.MI = Person.MI"
    }

    testTask "Col = Some Col Where" {
        let query = 
            select {
                for p in table<Person> do
                where (p.MI = Some p.LName)
            }
        
        Expect.equal query.Where (Expr "Person.MI = Person.LName") "Expected WHERE Person.MI = Person.LName"
    }

    testTask "Col = Constant Record.Property Value Where" {

        // This test ensures that "personInstance.Age" property value is treated as a real value,
        // Good: "Person.Age = 100"
        // Bad: "Person.Age = Person.Age".
        // In other words, it needs to be able to detect that this is Col = Member Value, not a column = column expression.
        let personInstance = { Person.Id = 123; FName = "Jordan"; LName = "Marr"; MI = None; Age = 100 }

        let query = 
            select {
                for p in table<Person> do
                where (p.Age < personInstance.Age)
            }
        
        Expect.equal query.Where (lt "Person.Age" 100) "Expected WHERE Age constant value of 100 to be unwrapped from property"
    }

    testTask "isNullValue Where" {
        let query = 
            select {
                for p in table<Person> do
                where (Builders.isNullValue p.Age || Builders.isNullValue p.MI || Builders.isNullValue p.FName)
            }
    
        Expect.equal query.Where (
            isNullValue "Person.Age" * isNullValue "Person.MI" * isNullValue "Person.FName"
        ) "Expected all three fields to check for NULL"
    }

    testTask "isNotNullValue Where" {
        let query = 
            select {
                for p in table<Person> do
                where (Builders.isNotNullValue p.Age || Builders.isNotNullValue p.MI || Builders.isNotNullValue p.FName)
            }
    
        Expect.equal query.Where (
            isNotNullValue "Person.Age" * isNotNullValue "Person.MI" * isNotNullValue "Person.FName"
        ) "Expected all three fields to check for NOT NULL"
    }

    testTask "Binary Where" {
        let query = 
            select {
                for p in table<Person> do
                where (p.FName = "John" && p.LName = "Doe")
                orderByDescending p.LName
                thenByDescending p.Age
            }
    
        Expect.equal query.Table "Person" "Expected table = 'Person'"
        Expect.equal query.Where (eq "Person.FName" "John" + eq "Person.LName" "Doe") "Expected WHERE Person.FName = 'John' && Person.LName = 'Doe'"
        Expect.equal query.OrderBy [("Person.LName", Desc); ("Person.Age", Desc)] "Expected ORDER BY 'Person.LName DESC, Person.Age DESC'"
    }

    testTask "Unary Not" {
        let query = 
            select {
                for p in table<Person> do
                where (not (p.FName = "John"))
            }
    
        Expect.equal query.Where (!!(eq "Person.FName" "John")) "Expected NOT (Person.FName = 'John')"
    }

    testTask "Group By" {
        let query = 
            select {
                for p in table<Person> do
                count "*" "Count"
                where (not (p.FName = "John"))
                groupBy p.Age
            }
    
        Expect.equal query.GroupBy ["Person.Age"] "Expected GROUP BY Person.Age"
        Expect.equal query.Aggregates [Count ("*", "Count")] "Expected COUNT(*) as [Count]"
    }

    testTask "Group By Many" {
        let query = 
            select {
                for p in table<Person> do
                innerJoin c in table<Contact> on (p.Id = c.PersonId)
                groupBy (p.FName, p.LName)
                count "*" "Count"
            }
    
        Expect.equal query.GroupBy ["Person.FName"; "Person.LName"] "Expected GROUP BY Person.FName, Person.LName"
        Expect.equal query.Aggregates [Count ("*", "Count")] "Expected COUNT(*) as [Count]"
    }
    
    testTask "Group By Optional Property" {
        let query = 
            select {
                for p in table<Person> do
                count "*" "Count"
                groupBy p.MI
            }
        
        Expect.equal query.GroupBy ["Person.MI"] "Expected GROUP BY Person.MI"
    }

    testTask "Optional Column is None" {
        let query = 
            select {
                for p in table<Person> do
                where (p.MI = None)
            }
    
        Expect.equal query.Where (isNullValue "Person.MI") "Expected Person.MI IS NULL"
    }

    testTask "Optional Column is not None" {
        let query = 
            select {
                for p in table<Person> do
                where (p.MI <> None)
            }
    
        Expect.equal query.Where (isNotNullValue "Person.MI") "Expected MI IS NOT NULL"
    }

    testTask "Optional Column = Some value" {
        let query = 
            select {
                for p in table<Person> do
                where (p.MI = Some "N")
            }
    
        Expect.equal query.Where (eq "Person.MI" "N") "Expected Person.MI = 'N'"
    }

    testTask "SqlMethods.isIn" {
        let query = 
            select {
                for p in table<Person> do
                where (Builders.isIn p.Age [18;21])
            }
    
        Expect.equal query.Where (Column ("Person.Age", In [18;21])) "Expected Person.Age IN (18,21)"
    }

    testTask "SqlMethods.isIn |=|" {
        let query = 
            select {
                for p in table<Person> do
                where (p.Age |=| [18;21])
            }
    
        Expect.equal query.Where (Column ("Person.Age", In [18;21])) "Expected Person.Age IN (18,21)"
    }

    testTask "SqlMethods.isIn with Optional List Values" {
        let query = 
            select {
                for p in table<Person> do
                where (p.MI.Value |=| [ "N"; "M" ])
            }
    
        Expect.equal query.Where (Column ("Person.MI", In [ "N"; "M" ])) "Expected Person.MI IN ('N', 'M')"
    }

    testTask "SqlMethods.isNotIn" {
        let ages = [1..5]
        let query = 
            select {
                for p in table<Person> do
                where (Builders.isNotIn p.Age ages)
            }
    
        Expect.equal query.Where (Column ("Person.Age", NotIn [1;2;3;4;5])) "Expected Person.Age NOT IN (1,2,3,4,5)"
    }

    testTask "SqlMethods.isNotIn |<>|" {
        let ages = [1..5]
        let query = 
            select {
                for p in table<Person> do
                where (p.Age |<>| ages)
            }
    
        Expect.equal query.Where (Column ("Person.Age", NotIn [1;2;3;4;5])) "Expected Person.Age NOT IN (1,2,3,4,5)"
    }

    testTask "Like" {
        let query = 
            select {
                for p in table<Person> do
                where (Builders.like p.LName "D%")
            }
    
        Expect.equal query.Where (Column ("Person.LName", Like "D%")) "Expected LName Person.LIKE \"D%\""
    }

    testTask "Like =%" {
        let query = 
            select {
                for p in table<Person> do
                where (p.LName =% "D%")
            }
    
        Expect.equal query.Where (Column ("Person.LName", Like "D%")) "Expected LName Person.LIKE \"D%\""
    }

    testTask "Not Like" {
        let query = 
            select {
                for p in table<Person> do
                where (Builders.notLike p.LName "D%")
            }
    
        Expect.equal query.Where (Column ("Person.LName", NotLike "D%")) "Expected LName Person.NotLike \"D%\""
    }

    testTask "Not Like <>%" {
        let query = 
            select {
                for p in table<Person> do
                where (p.LName <>% "D%")
            }
    
        Expect.equal query.Where (Column ("Person.LName", NotLike "D%")) "Expected LName Person.NotLike \"D%\""
    }

    testTask "Count" {
        let query = 
            select {
                for p in table<Person> do
                count "*" "Count"
                where (not (p.FName = "John"))
            }
    
        Expect.equal query.Aggregates [Count ("*", "Count")] "Expected count(*) as [Count]"
    }
    
    //testTask "Max By" {
    //    let query = 
    //        select {
    //            for p in entity do
    //            maxBy p.Age
    //        }
    
    //    Expect.equal query.Aggregates [Max ("Person.Age", "Person.Age")] "Expected max(Age) as [Age]"
    //}
    
    testTask "Join" {
        let query = 
            select {
                for p in table<Person> do
                innerJoin a in table<Address> on (p.Id = a.PersonId) 
                where (p.Id = 1 && a.PersonId = 2) 
            }
    
        Expect.equal query.Joins [InnerJoin ("Address", ["PersonId", EqualsToColumn "Person.Id"])] "Expected INNER JOIN Address ON Person.Id = Address.PersonId"
        Expect.equal query.Where (eq "Person.Id" 1 + eq "Address.PersonId" 2) "Expected both types in where clause"
    }
    
    testTask "Join2" {
        let query = 
            select {
                for p in table<Person> do
                innerJoin a in table<Address> on (p.Id = a.PersonId) 
                innerJoin c in table<Contact> on (p.Id = c.PersonId)
                selectAll
            }
    
        Expect.equal query.Joins [
            InnerJoin ("Address", ["PersonId", EqualsToColumn "Person.Id"])
            InnerJoin ("Contact", ["PersonId", EqualsToColumn "Person.Id"])
        ] "Expected INNER JOIN Address ON Person.Id = Address.PersonId"
    }

    testTask "LeftJoin" {
        let query = 
            select {
                for p in table<Person> do
                leftJoin a in table<Address> on (p.Id = a.PersonId) 
                where (p.Id = 1 && a.PersonId = 2) 
            }
    
        Expect.equal query.Joins [LeftJoin ("Address", ["PersonId", EqualsToColumn "Person.Id"])] "Expected LEFT JOIN Address ON Person.Id = Address.PersonId"
        Expect.equal query.Where (eq "Person.Id" 1 + eq "Address.PersonId" 2) "Expected both types in where clause"
    }

    testTask "LeftJoin2" {        
        let query = 
            select {
                for p in table<Person> do
                leftJoin a in table<Address> on (p.Id = a.PersonId) 
                leftJoin c in table<Contact> on (p.Id = c.PersonId)
                where (p.Id = 1 && a.PersonId = 2 && c.Phone = "919-765-4321")
            }
    
        Expect.equal query.Joins [
            LeftJoin ("Address", ["PersonId", EqualsToColumn "Person.Id"])
            LeftJoin ("Contact", ["PersonId", EqualsToColumn "Person.Id"])
        ] "Expected LEFT JOIN Address ON Person.Id = Address.PersonId"
        Expect.equal query.Where (eq "Person.Id" 1 + eq "Address.PersonId" 2 + eq "Contact.Phone" "919-765-4321") "Expected all types in where clause"
    }

    testTask "Join2 with Custom Schema and Table Names" {
        let personTable = table'<Person> "People" |> inSchema "dbo"
        let addressTable = table'<Address> "Addresses" |> inSchema "dbo"
        let contactTable = table'<Contact> "Contacts" |> inSchema "dbo"

        let query = 
            select {
                for p in personTable do
                innerJoin a in addressTable on (p.Id = a.PersonId) 
                innerJoin c in contactTable on (p.Id = c.PersonId)
                where (p.FName = "John" && a.City = "Chicago" && c.Phone = "919-765-4321")
                orderByDescending p.Id
                thenBy a.City
                thenBy c.Phone
            }
    
        Expect.equal query.Schema (Some "dbo") "Expected schema = dbo"
        Expect.equal query.Table "People" "Expected table = People"
        Expect.equal query.Joins [
            InnerJoin ("dbo.Addresses", ["PersonId", EqualsToColumn "dbo.People.Id"])
            InnerJoin ("dbo.Contacts", ["PersonId", EqualsToColumn "dbo.People.Id"])
        ] "Expected tables and columns to be fully qualified with schema and overriden table names"
        Expect.equal query.Where 
            (eq "dbo.People.FName" "John" + eq "dbo.Addresses.City" "Chicago" + eq "dbo.Contacts.Phone" "919-765-4321") 
            "Expected tables and columns to be fully qualified with schema and overriden table names"
        Expect.equal query.OrderBy [
            OrderBy ("dbo.People.Id", Desc)
            OrderBy ("dbo.Addresses.City", Asc)
            OrderBy ("dbo.Contacts.Phone", Asc)
        ] "Expected tables and columns to be fully qualified with schema and overriden table names"
    }

    testTask "LeftJoin2 with Custom Schema and Table Names" {
        let personTable = table'<Person> "People" |> inSchema "dbo"
        let addressTable = table'<Address> "Addresses" |> inSchema "dbo"
        let contactTable = table'<Contact> "Contacts" |> inSchema "dbo"
                                                                    
        let query = 
            select {
                for p in personTable do
                leftJoin a in addressTable on (p.Id = a.PersonId) 
                leftJoin c in contactTable on (p.Id = c.PersonId)
                where (p.FName = "John" && a.City = "Chicago" && c.Phone = "919-765-4321")
                orderBy p.Id
                thenBy a.City
                thenByDescending c.Phone
            }
    
        Expect.equal query.Schema (Some "dbo") "Expected schema = dbo"
        Expect.equal query.Table "People" "Expected table = People"
        Expect.equal query.Joins [
            LeftJoin ("dbo.Addresses", ["PersonId", EqualsToColumn "dbo.People.Id"])
            LeftJoin ("dbo.Contacts", ["PersonId", EqualsToColumn "dbo.People.Id"])
        ] "Expected tables and columns to be fully qualified with schema and overriden table names"
        Expect.equal query.Where 
            (eq "dbo.People.FName" "John" + eq "dbo.Addresses.City" "Chicago" + eq "dbo.Contacts.Phone" "919-765-4321") 
            "Expected tables and columns to be fully qualified with schema and overriden table names"
        Expect.equal query.OrderBy [
            OrderBy ("dbo.People.Id", Asc)
            OrderBy ("dbo.Addresses.City", Asc)
            OrderBy ("dbo.Contacts.Phone", Desc)
        ] "Expected tables and columns to be fully qualified with schema and overriden table names"
    }

    testTask "Join should unwrap option types in 'on' condition" {
        let personTable = table'<Person> "People" |> inSchema "dbo"
        let addressTable = table'<Address> "Addresses" |> inSchema "dbo"
        let contactTable = table'<Contact> "Contacts" |> inSchema "dbo"

        // This is a nonsensical innerJoin, but the point is to test unwrapping MI option type in join "on"
        let query = 
            select {
                for p in personTable do
                innerJoin a in addressTable on (p.MI = Some a.City) 
                selectAll
            }

        Expect.equal query.Joins [
            InnerJoin ("dbo.Addresses", ["City", EqualsToColumn "dbo.People.MI"])
        ] "Expected that option column (MI) should be unwrapped."
    }
    
    testTask "Join On Value Bug Fix Test" {
        let personTable = table'<Person> "People" |> inSchema "dbo"
        let addressTable = table'<Address> "Addresses" |> inSchema "dbo"
    
        // This is a nonsensical join, but the point is to test joining on an optional property `.Value` (p.MI.Value)
        let query = 
            select {
                for p in personTable do
                innerJoin a in addressTable on (p.MI.Value = a.City) 
                selectAll
            }
    
        Expect.equal query.Joins [
            InnerJoin ("dbo.Addresses", ["City", EqualsToColumn "dbo.People.MI"])
        ] "Expected that option column (MI) should be unwrapped."
    }

    testTask "Inner Join Multi-Column" {
        let query = 
            select {
                for l in table<MultiJoinLeft> do
                innerJoin r in table<MultiJoinRight> on ((l.Key1, l.Key2) = (r.Key1, r.Key2)) 
                selectAll
            }

        Expect.equal query.Joins [
            InnerJoin ("MultiJoinRight", ["Key1", EqualsToColumn "MultiJoinLeft.Key1"; "Key2", EqualsToColumn "MultiJoinLeft.Key2"])
        ] ""
    }
    
    ftestTask "Inner Join with Constant Value" {
        let query = 
            select {
                for l in table<MultiJoinLeft> do
                innerJoin r in table<MultiJoinRight> on (5L = r.Key2) 
                selectAll
            }

        Expect.equal query.Joins [
            InnerJoin ("MultiJoinRight", ["Key2", EqualsToConstant 5L])
        ] ""
    }

    
    ftestTask "Inner Join Multi-Column with Constant Values" {
        let query = 
            select {
                for l in table<MultiJoinLeft> do
                innerJoin r in table<MultiJoinRight> on ((3, 5L) = (r.Key1, r.Key2)) 
                selectAll
            }

        Expect.equal query.Joins [
            InnerJoin ("MultiJoinRight", ["Key1", EqualsToConstant 3; "Key2", EqualsToConstant 5L])
        ] ""
    }

    testTask "Left Join Multi-Column" {
        let query = 
            select {
                for l in table<MultiJoinLeft> do
                leftJoin r in table<MultiJoinRight> on ((l.Key1, l.Key2) = (r.Key1, r.Key2)) 
                selectAll
            }

        Expect.equal query.Joins [
            LeftJoin ("MultiJoinRight", ["Key1", EqualsToColumn "MultiJoinLeft.Key1"; "Key2", EqualsToColumn "MultiJoinLeft.Key2"])
        ] ""
    }

    testTask "Insert with 1 excluded field" {
        let person = 
            { Id = 0
              FName = "John"
              MI = None
              LName = "Doe"
              Age = 100 }
    
        let query =
            insert {
                for p in table<Person> do
                value person
                excludeColumn p.Id
            }
            
        Expect.equal query.Fields ["FName"; "MI"; "LName"; "Age"] "Expected all fields except 'Id'."
    }
    
    testTask "Insert with 2 excluded fields" {
        let person = 
            { Id = 0
              FName = "John"
              MI = None
              LName = "Doe"
              Age = 100 }
    
        let query =
            insert {
                for p in table<Person> do
                value person
                excludeColumn p.Id
                excludeColumn p.MI
            }
            
        Expect.equal query.Fields ["FName"; "LName"; "Age"] "Expected all fields except 'Id' and 'MI'."
    }

    

    testTask "Update with 2 excluded fields" {
        let person = 
            { Id = 1
              FName = "John"
              MI = None
              LName = "Doe"
              Age = 100 }
    
        let query =
            update {
                for p in table<Person> do
                set person
                excludeColumn p.Id
                excludeColumn p.MI
            }
            
        Expect.equal query.Fields ["FName"; "LName"; "Age"] "Expected all fields except 'Id' and 'MI'."
    }

    

    testTask "Guid in Where" {
        let query = 
            select {
                for v in table<Vehicle> do
                where (v.Id = System.Guid("c586871d-3329-4fca-a231-fd11203a937d"))
            }

        Expect.equal query.Where (eq "Vehicle.Id" (System.Guid("c586871d-3329-4fca-a231-fd11203a937d"))) "Expected WHERE to contain a guid"
    }
]

