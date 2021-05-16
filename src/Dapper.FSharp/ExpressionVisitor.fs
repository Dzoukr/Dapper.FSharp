module internal Dapper.FSharp.ExpressionVisitor

open System.Linq.Expressions
open System
open System.Reflection

let notImpl() = raise (NotImplementedException())
let notImplMsg msg = raise (NotImplementedException msg)

let isOptionType (t: Type) = 
    t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Option<_>>

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

let (|New|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.New -> Some (exp :?> NewExpression)
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
    | ExpressionType.Equal when (isNull value) -> IsNull
    | ExpressionType.NotEqual when (isNull value) -> IsNotNull
    | ExpressionType.Equal -> Eq value
    | ExpressionType.NotEqual -> Ne value
    | ExpressionType.GreaterThan -> Gt value
    | ExpressionType.GreaterThanOrEqual -> Ge value
    | ExpressionType.LessThan -> Lt value
    | ExpressionType.LessThanOrEqual -> Le value
    | _ -> notImplMsg "Unsupported comparison type"

let getComparison (expType: ExpressionType) =
    match expType with
    | ExpressionType.Equal -> "="
    | ExpressionType.NotEqual -> "<>"
    | ExpressionType.GreaterThan -> ">"
    | ExpressionType.GreaterThanOrEqual -> ">="
    | ExpressionType.LessThan -> "<"
    | ExpressionType.LessThanOrEqual -> "<="
    | _ -> notImplMsg "Unsupported comparison type"

let rec unwrapListExpr (lstValues: obj list, lstExp: MethodCallExpression) =
    if lstExp.Arguments.Count > 0 then
        match lstExp.Arguments.[0] with
        | Constant c -> unwrapListExpr (lstValues @ [c.Value], (lstExp.Arguments.[1] :?> MethodCallExpression))
        | _ -> notImpl()
    else 
        lstValues

let visitWhere<'T> (filter: Expression<Func<'T, bool>>) (qualifyColumn: MemberInfo -> string) =
    let rec visit (exp: Expression) : Where =
        match exp with
        | Lambda x -> visit x.Body
        | Unary x -> 
            match x.NodeType with
            | ExpressionType.Not -> 
                let operand = visit x.Operand
                Unary (Not, operand)
            | _ ->
                notImplMsg "Unsupported unary operation"
        | MethodCall m when m.Method.Name = "Invoke" ->
            // Handle tuples
            visit m.Object
        | MethodCall m when m.Method.Name = "isIn" || m.Method.Name = "isNotIn" ->
            let comparisonType = if m.Method.Name = "isIn" then In else NotIn
            match m.Arguments.[0], m.Arguments.[1] with
            | Member col, MethodCall lst ->
                let lstValues = unwrapListExpr ([], lst)                
                Column (qualifyColumn col.Member, comparisonType lstValues)
            | Member col, Constant c -> 
                let lstValues = (c.Value :?> System.Collections.IEnumerable) |> Seq.cast<obj> |> Seq.toList
                Column (qualifyColumn col.Member, comparisonType lstValues)
            | _ -> notImpl()
        | MethodCall m when m.Method.Name = "like" ->
            match m.Arguments.[0], m.Arguments.[1] with
            | Member col, Constant c -> 
                let pattern = string c.Value
                Column (qualifyColumn col.Member, Like pattern)
            | _ -> notImpl()
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
                | Member col1, Member col2 ->
                    // Handle col to col comparisons
                    let lt = qualifyColumn col1.Member
                    let cp = getComparison exp.NodeType
                    let rt = qualifyColumn col2.Member
                    Expr (sprintf "%s %s %s" lt cp rt)
                | Constant _, Constant _ ->
                    notImplMsg("Constant to Constant comparisons are not currently supported. Ex: 'where (1 = 1)'")
                | Member col, Constant c
                | Constant c, Member col ->
                    // Handle regular column comparisons
                    let value = c.Value
                    let columnComparison = getColumnComparison(exp.NodeType, value)
                    Column (qualifyColumn col.Member, columnComparison)
                | Member col, MethodCall c when c.Type |> isOptionType ->
                    // Handle optional column comparisons
                    if c.Arguments.Count > 0 then 
                        match c.Arguments.[0] with
                        | Constant optVal -> 
                            let columnComparison = getColumnComparison(exp.NodeType, optVal.Value)
                            Column (qualifyColumn col.Member, columnComparison)
                        | _ -> 
                            notImpl()
                    else
                        let columnComparison = getColumnComparison(exp.NodeType, null)
                        Column (qualifyColumn col.Member, columnComparison)
                | _ ->
                    notImpl()
        | _ ->
            notImpl()

    visit (filter :> Expression)

/// Returns a list of one or more fully qualified column names: ["{schema}.{table}.{column}"]
let visitGroupBy<'T, 'Prop> (propertySelector: Expression<Func<'T, 'Prop>>) (qualifyColumn: MemberInfo -> string) =
    let rec visit (exp: Expression) : string list =
        match exp with
        | Lambda x -> visit x.Body
        | MethodCall m when m.Method.Name = "Invoke" ->
            // Handle tuples
            visit m.Object
        | New n -> 
            // Handle groupBy that returns a tuple of multiple columns
            n.Arguments |> Seq.map visit |> Seq.toList |> List.concat
        | Member m -> 
            // Handle groupBy for a single column
            let column = qualifyColumn m.Member
            [column]
        | _ -> notImpl()

    visit (propertySelector :> Expression)


/// Returns a fully qualified column name: "{schema}.{table}.{column}"
let visitPropertySelector<'T, 'Prop> (propertySelector: Expression<Func<'T, 'Prop>>) (qualifyColumn: MemberInfo -> string) =
    let rec visit (exp: Expression) : string =
        match exp with
        | Lambda x -> visit x.Body
        | MethodCall m when m.Method.Name = "Invoke" ->
            // Handle tuples
            visit m.Object
        | Member m -> 
            qualifyColumn m.Member
        | _ -> notImpl()

    visit (propertySelector :> Expression)
