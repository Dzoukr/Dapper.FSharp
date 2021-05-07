module internal Dapper.FSharp.ExpressionVisitor

open System.Linq.Expressions
open System

let (|Lambda|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Lambda -> Some (exp :?> LambdaExpression)
    | _ -> None

let (|Unary|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.ArrayLength
    | ExpressionType.Convert
    | ExpressionType.ConvertChecked
    | ExpressionType.Negate
    | ExpressionType.UnaryPlus
    | ExpressionType.NegateChecked
    | ExpressionType.Not
    | ExpressionType.Quote
    | ExpressionType.TypeAs -> Some (exp :?> UnaryExpression)
    | _ -> None

let (|Binary|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Add
    | ExpressionType.AddChecked
    | ExpressionType.And
    | ExpressionType.AndAlso
    | ExpressionType.ArrayIndex
    | ExpressionType.Coalesce
    | ExpressionType.Divide
    | ExpressionType.Equal
    | ExpressionType.ExclusiveOr
    | ExpressionType.GreaterThan
    | ExpressionType.GreaterThanOrEqual
    | ExpressionType.LeftShift
    | ExpressionType.LessThan
    | ExpressionType.LessThanOrEqual
    | ExpressionType.Modulo
    | ExpressionType.Multiply
    | ExpressionType.MultiplyChecked
    | ExpressionType.NotEqual
    | ExpressionType.Or
    | ExpressionType.OrElse
    | ExpressionType.Power
    | ExpressionType.RightShift
    | ExpressionType.Subtract
    | ExpressionType.SubtractChecked -> Some (exp :?> BinaryExpression)
    | _ -> None

let (|MethodCall|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Call -> Some (exp :?> MethodCallExpression)
    | _ -> None

let (|Constant|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Constant -> Some (exp :?> ConstantExpression)
    | _ -> None

let (|Member|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.MemberAccess -> Some (exp :?> MemberExpression)
    | _ -> None

let (|Parameter|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Parameter -> Some (exp :?> ParameterExpression)
    | _ -> None

let getColumnComparison (expType: ExpressionType, value: obj) =
    match expType with
    | ExpressionType.Equal -> Eq value
    | ExpressionType.NotEqual -> Ne value
    | ExpressionType.GreaterThan -> Gt value
    | ExpressionType.GreaterThanOrEqual -> Ge value
    | ExpressionType.LessThan -> Lt value
    | ExpressionType.LessThanOrEqual -> Le value
    | _ -> raise (NotImplementedException "Unsupported comparison type")

let visitWhere<'T> (filter: Expression<Func<'T, bool>>) =
    let rec visit (exp: Expression) : Where =
        match exp with
        | Lambda x -> visit x.Body
        | Unary x -> 
            match x.NodeType with
            | ExpressionType.Not -> 
                let operand = visit x.Operand
                Unary (Not, operand)
            | _ ->
                raise (NotImplementedException "Unsupported unary operation")
        | Binary x -> 
            match exp.NodeType with
            | ExpressionType.And
            | ExpressionType.AndAlso ->
                let lt = visit x.Left
                let rt = visit x.Right
                Binary (lt, And, rt)
            | ExpressionType.Or
            | ExpressionType.OrElse ->
                let lt = visit x.Left
                let rt = visit x.Right
                Binary (lt, Or, rt)
            | _ ->
                match x.Left, x.Right with
                | Member m, Constant c
                | Constant c, Member m ->
                    let colName = m.Member.Name
                    let value = c.Value
                    let columnComparison = getColumnComparison(exp.NodeType, value)
                    Column (colName, columnComparison)
                | _ ->
                    raise (NotImplementedException())
        | _ ->
            raise (NotImplementedException())

    visit (filter :> Expression)

let visitOrderBy<'T, 'TProp> (propertySelector: Expression<Func<'T, 'TProp>>, direction) =
    let rec visit (exp: Expression) : OrderBy =
        match exp with
        | Lambda x -> visit x.Body
        | Member m -> OrderBy (m.Member.Name, direction)
        | _ -> raise (NotImplementedException())

    visit (propertySelector :> Expression)

let visitGroupBy<'T, 'TProp> (propertySelector: Expression<Func<'T, 'TProp>>) =
    let rec visit (exp: Expression) : string =
        match exp with
        | Lambda x -> visit x.Body
        | Member m -> m.Member.Name
        | _ -> raise (NotImplementedException())

    visit (propertySelector :> Expression)