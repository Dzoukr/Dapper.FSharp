module Dapper.FSharp.Tests.LinqSelectTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.Tests.Database
open Dapper.FSharp.LinqBuilders
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

let unitTests() = testList "LINQ SELECT UNIT TESTS" [
    
    testTask "Simple Query" {
        let query = 
            select {
                for p in entity<Person> do
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
                for p in entity<Person> do
                where (p.FName = p.LName)
            }
        
        Expect.equal query.Where (Expr "Person.FName = Person.LName") "Expected WHERE Person.FName = Person.LName"
    }

    testTask "Col option = Col option Where" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI = p.MI)
            }
        
        Expect.equal query.Where (Expr "Person.MI = Person.MI") "Expected WHERE Person.MI = Person.MI"
    }

    testTask "Value = Value Where" {
        let query = 
            select {
                for p in entity<Person> do
                where (1 = 1)
            }
        
        Expect.equal query.Where (Expr "1 = 1") "Expected WHERE 1 = 1"
    }

    testTask "Col = Some Col Where" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI = Some p.LName)
            }
        
        Expect.equal query.Where (Expr "Person.MI = Person.LName") "Expected WHERE Person.MI = Person.LName"
    }

    testTask "Binary Where" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.FName = "John" && p.LName = "Doe")
                orderByDescending p.LName
                orderByDescending p.Age
            }
    
        Expect.equal query.Table "Person" "Expected table = 'Person'"
        Expect.equal query.Where (eq "Person.FName" "John" + eq "Person.LName" "Doe") "Expected WHERE Person.FName = 'John' && Person.LName = 'Doe'"
        Expect.equal query.OrderBy [("Person.LName", Desc); ("Person.Age", Desc)] "Expected ORDER BY 'Person.LName DESC, Person.Age DESC'"
    }

    testTask "Unary Not" {
        let query = 
            select {
                for p in entity<Person> do
                where (not (p.FName = "John"))
            }
    
        Expect.equal query.Where (!!(eq "Person.FName" "John")) "Expected NOT (Person.FName = 'John')"
    }

    testTask "Group By" {
        let query = 
            select {
                for p in entity<Person> do
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
                for p in entity<Person> do
                join c in entity<Contact> on (p.Id = c.PersonId)
                groupBy (p.FName, p.LName)
                count "*" "Count"
            }
    
        Expect.equal query.GroupBy ["Person.FName"; "Person.LName"] "Expected GROUP BY Person.FName, Person.LName"
        Expect.equal query.Aggregates [Count ("*", "Count")] "Expected COUNT(*) as [Count]"
    }

    testTask "Optional Column is None" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI = None)
            }
    
        Expect.equal query.Where (isNullValue "Person.MI") "Expected Person.MI IS NULL"
    }

    testTask "Optional Column is not None" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI <> None)
            }
    
        Expect.equal query.Where (isNotNullValue "Person.MI") "Expected MI IS NOT NULL"
    }

    testTask "Optional Column = Some value" {
        let query = 
            select {
                for p in entity<Person> do
                where (p.MI = Some "N")
            }
    
        Expect.equal query.Where (eq "Person.MI" "N") "Expected Person.MI = 'N'"
    }

    testTask "SqlMethods.isIn" {
        let query = 
            select {
                for p in entity<Person> do
                where (isIn p.Age [18;21])
            }
    
        Expect.equal query.Where (Column ("Person.Age", In [18;21])) "Expected Person.Age IN (18,21)"
    }

    testTask "SqlMethods.isNotIn" {
        let ages = [1..5]
        let query = 
            select {
                for p in entity<Person> do
                where (isNotIn p.Age ages)
            }
    
        Expect.equal query.Where (Column ("Person.Age", NotIn [1;2;3;4;5])) "Expected Person.Age NOT IN (1,2,3,4,5)"
    }

    testTask "Like" {
        let query = 
            select {
                for p in entity<Person> do
                where (like p.LName "D%")
            }
    
        Expect.equal query.Where (Column ("Person.LName", Like "D%")) "Expected LName Person.LIKE \"D%\""
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
                for p in entity do
                maxBy p.Age
            }
    
        Expect.equal query.Aggregates [Max ("Person.Age", "Person.Age")] "Expected max(Age) as [Age]"
    }
    
    testTask "Join" {
        let query = 
            select {
                for p in entity<Person> do
                join a in entity<Address> on (p.Id = a.PersonId) 
                where (p.Id = 1 && a.PersonId = 2) 
            }
    
        Expect.equal query.Joins [InnerJoin ("Address", "PersonId", "Person.Id")] "Expected INNER JOIN Address ON Person.Id = Address.PersonId"
        Expect.equal query.Where (eq "Person.Id" 1 + eq "Address.PersonId" 2) "Expected both types in where clause"
    }
    
    testTask "Join2" {
        let query = 
            select {
                for p in entity<Person> do
                join a in entity<Address> on (p.Id = a.PersonId) 
                join c in entity<Contact> on (p.Id = c.PersonId)
                selectAll
            }
    
        Expect.equal query.Joins [
            InnerJoin ("Address", "PersonId", "Person.Id")
            InnerJoin ("Contact", "PersonId", "Person.Id")
        ] "Expected INNER JOIN Address ON Person.Id = Address.PersonId"
    }

    testTask "LeftJoin" {
        let query = 
            select {
                for p in entity<Person> do
                leftJoin a in entity<Address> on (p.Id = a.PersonId) 
                where (p.Id = 1 && a.PersonId = 2) 
            }
    
        Expect.equal query.Joins [LeftJoin ("Address", "PersonId", "Person.Id")] "Expected LEFT JOIN Address ON Person.Id = Address.PersonId"
        Expect.equal query.Where (eq "Person.Id" 1 + eq "Address.PersonId" 2) "Expected both types in where clause"
    }

    testTask "LeftJoin2" {        
        let query = 
            select {
                for p in entity<Person> do
                leftJoin a in entity<Address> on (p.Id = a.PersonId) 
                leftJoin c in entity<Contact> on (p.Id = c.PersonId)
                where (p.Id = 1 && a.PersonId = 2 && c.Phone = "919-765-4321")
            }
    
        Expect.equal query.Joins [
            LeftJoin ("Address", "PersonId", "Person.Id")
            LeftJoin ("Contact", "PersonId", "Person.Id")
        ] "Expected LEFT JOIN Address ON Person.Id = Address.PersonId"
        Expect.equal query.Where (eq "Person.Id" 1 + eq "Address.PersonId" 2 + eq "Contact.Phone" "919-765-4321") "Expected all types in where clause"
    }

    testTask "Join2 with Custom Schema and Table Names" {
        let personTable = entity<Person> |> mapSchema "dbo"  |> mapTable "People"
        let addressTable = entity<Address> |> mapSchema "dbo"  |> mapTable "Addresses"
        let contactTable = entity<Contact> |> mapSchema "dbo"  |> mapTable "Contacts"

        let query = 
            select {
                for p in personTable do
                join a in addressTable on (p.Id = a.PersonId) 
                join c in contactTable on (p.Id = c.PersonId)
                where (p.FName = "John" && a.City = "Chicago" && c.Phone = "919-765-4321")
                orderByDescending p.Id
                thenBy a.City
                thenBy c.Phone
            }
    
        Expect.equal query.Schema (Some "dbo") "Expected schema = dbo"
        Expect.equal query.Table "People" "Expected table = People"
        Expect.equal query.Joins [
            InnerJoin ("dbo.Addresses", "PersonId", "dbo.People.Id")
            InnerJoin ("dbo.Contacts", "PersonId", "dbo.People.Id")
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
        let personTable = entity<Person> |> mapTable "People" |> mapSchema "dbo"
        let addressTable = entity<Address> |> mapTable "Addresses" |> mapSchema "dbo"
        let contactTable = entity<Contact> |> mapTable "Contacts" |> mapSchema "dbo"
                                                                    
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
            LeftJoin ("dbo.Addresses", "PersonId", "dbo.People.Id")
            LeftJoin ("dbo.Contacts", "PersonId", "dbo.People.Id")
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
]

let integrationTests (crud:ICrud) (init:ICrudInitializer) = testList "LINQ SELECT INTEGRATION TESTS" [

    let personsView = entity<Persons.View> |> mapTable "Persons"
    let dogsView = entity<Dogs.View> |> mapTable "Dogs"
    let dogsWeights = entity<DogsWeights.View> |> mapTable "DogsWeights"

    testTask "Selects by single where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Position = 5)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
    }

    testTask "Selects by single where condition with table name used" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Position = 5)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
    }

    testTask "Selects by IN where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (isIn p.Position [5;6])
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 5)) (Seq.head fromDb) ""
        Expect.equal (rs |> List.find (fun x -> x.Position = 6)) (Seq.last fromDb) ""
    }

    testTask "Selects by NOT IN where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (isNotIn p.Position [1;2;3])
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 4)) (Seq.head fromDb) ""
        Expect.equal (rs |> List.find (fun x -> x.Position = 10)) (Seq.last fromDb) ""
        Expect.equal 7 (Seq.length fromDb) ""
    }

    testTask "Selects by IS NULL where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.DateOfBirth = None)
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 2)) (Seq.head fromDb) ""
        Expect.equal 5 (Seq.length fromDb) ""
    }

    testTask "Selects by IS NOT NULL where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.DateOfBirth <> None)
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 1)) (Seq.head fromDb) ""
        Expect.equal 5 (Seq.length fromDb) ""
    }

    testTask "Selects by LIKE where condition return matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (like p.FirstName "First_1%")
            } |> crud.SelectAsync<Persons.View>
        Expect.isNonEmpty fromDb ""
        Expect.hasLength fromDb 2 ""
        Expect.isTrue (fromDb |> Seq.forall (fun (p:Persons.View) -> p.FirstName.StartsWith "First")) ""
    }
    
    testTask "Selects by NOT LIKE where condition return matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (notLike p.FirstName "First_1%")
            }
            |> crud.SelectAsync<Persons.View>
        Expect.hasLength fromDb 8 ""
    }

    testTask "Selects by NOT LIKE where condition do not return non-matching rows" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (notLike p.FirstName "NonExistingName%")
            } |> crud.SelectAsync<Persons.View>
        Expect.hasLength fromDb 10 ""
    }

    testTask "Selects by UNARY NOT where condition" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (not(p.Position > 5 && p.DateOfBirth = None))
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 9)) (Seq.last fromDb) ""
        Expect.equal 7 (Seq.length fromDb) ""
    }

    testTask "Selects by multiple where conditions" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Position > 2 && p.Position < 4)
            } |> crud.SelectAsync<Persons.View>
        Expect.equal (rs |> List.find (fun x -> x.Position = 3)) (Seq.head fromDb) ""
    }

    testTask "Selects with order by" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                orderByDescending p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 10 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
    }

    testTask "Selects with skip parameter" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                skip 5
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 5 (fromDb |> Seq.length) ""
    }

    testTask "Selects with skipTake parameter" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                skipTake 5 2
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 2 (fromDb |> Seq.length) ""
    }
    
    testTask "Selects with skip and take parameters" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                table "Persons"
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                skip 5
                take 2
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 6 (fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position)) ""
        Expect.equal 2 (fromDb |> Seq.length) ""
    }

    testTask "Selects with one inner join - 1:1" {
        do! init.InitPersons()
        do! init.InitDogs()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1to1 persons
        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                join d in dogsView on (p.Id = d.OwnerId)
                selectAll
            } |> crud.SelectAsync<Persons.View, Dogs.View>

        Expect.equal 10 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
    }

    testTask "Selects with one inner join - 1:N" {
        do! init.InitPersons()
        do! init.InitDogs()
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                join d in dogsView on (p.Id = d.OwnerId)
                selectAll
            } |> crud.SelectAsync<Persons.View, Dogs.View>

        let byOwner = fromDb |> Seq.groupBy fst

        Expect.equal 5 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head) (Seq.head fromDb) ""
        Expect.equal 1 (Seq.length byOwner) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
    }

    testTask "Selects with one left join" {
        do! init.InitPersons()
        do! init.InitDogs()
        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! fromDb =
            select {
                for p in personsView do
                join d in dogsView on (p.Id = d.OwnerId)
                orderBy p.Position
                thenBy d.Nickname
            } |> crud.SelectAsyncOption<Persons.View, Dogs.View>

        let byOwner = fromDb |> Seq.groupBy fst

        Expect.equal 14 (Seq.length fromDb) ""
        Expect.equal 5 (byOwner |> Seq.head |> snd |> Seq.length) ""
        Expect.isTrue (fromDb |> Seq.last |> snd |> Option.isNone) ""
        Expect.equal (dogs |> List.head |> Some) (fromDb |> Seq.head |> snd) ""
    }

    testTask "Selects with two inner joins - 1:1" {
        do! init.InitPersons()
        do! init.InitDogs()
        do! init.InitDogsWeights()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1to1 persons
        let weights = DogsWeights.View.generate1to1 dogs

        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                for p in personsView do
                join d in dogsView on (p.Id = d.OwnerId)
                join dw in dogsWeights on (d.Nickname = dw.DogNickname)
                orderBy p.Position
            } |> crud.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>

        Expect.equal 10 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head, weights.Head) (Seq.head fromDb) ""
    }

    testTask "Selects with two inner joins - 1:N" {
        do! init.InitPersons()
        do! init.InitDogs()
        do! init.InitDogsWeights()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let weights = DogsWeights.View.generate1toN 3 dogs.Head

        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                for p in personsView do
                join d in dogsView on (p.Id = d.OwnerId)
                join dw in dogsWeights on (d.Nickname = dw.DogNickname)
                orderBy p.Position
                thenBy d.Nickname
                thenBy dw.Year
            } |> crud.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>

        Expect.equal 3 (Seq.length fromDb) ""
        Expect.equal (persons.Head, dogs.Head, weights.Head) (Seq.head fromDb) ""
    }

    testTask "Selects with two left joins" {
        do! init.InitPersons()
        do! init.InitDogs()
        do! init.InitDogsWeights()

        let persons = Persons.View.generate 10
        let dogs = Dogs.View.generate1toN 5 persons.Head
        let weights = DogsWeights.View.generate1toN 3 dogs.Head

        let! _ =
            insert {
                table "Persons"
                values persons
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "Dogs"
                values dogs
            } |> crud.InsertAsync
        let! _ =
            insert {
                table "DogsWeights"
                values weights
            } |> crud.InsertAsync

        let! fromDb =
            select {
                for p in personsView do
                join d in dogsView on (p.Id = d.OwnerId)
                join dw in dogsWeights on (d.Nickname = dw.DogNickname)
                orderBy p.Position
                thenBy d.Nickname
                thenBy dw.Year
            } |> crud.SelectAsyncOption<Persons.View, Dogs.View, DogsWeights.View>

        let p1,d1,w1 = fromDb |> Seq.head
        Expect.equal persons.Head p1 ""
        Expect.equal (Some dogs.Head) d1 ""
        Expect.equal (Some weights.Head) w1 ""

        let pn,dn,wn = fromDb |> Seq.last
        Expect.equal (persons |> Seq.last) pn ""
        Expect.equal None dn ""
        Expect.equal None wn ""
        Expect.equal 16 (Seq.length fromDb) ""
    }
]