/// Added temporarily to fix tests
module Dapper.FSharp.Legacy

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
