module internal Dapper.FSharp.ExpressionVisitor

open System.Linq.Expressions
open System

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

let rec unwrapListExpr (lstValues: obj list, lstExp: MethodCallExpression) =
    if lstExp.Arguments.Count > 0 then
        match lstExp.Arguments.[0] with
        | Constant c -> unwrapListExpr (lstValues @ [c.Value], (lstExp.Arguments.[1] :?> MethodCallExpression))
        | _ -> notImpl()
    else 
        lstValues

let visitWhere<'T> (filter: Expression<Func<'T, bool>>) =
    /// Creates a qualified {table}.{column}
    let qualifiedColumn (col: MemberExpression, comparison) = 
        Column ((sprintf "%s.%s" col.Member.DeclaringType.Name col.Member.Name), comparison)

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
            // Handle Join2 and Join3 tuples
            visit m.Object
        | MethodCall m when m.Method.Name = "isIn" || m.Method.Name = "isNotIn" ->
            let comparisonType = if m.Method.Name = "isIn" then In else NotIn
            match m.Arguments.[0], m.Arguments.[1] with
            | Member col, MethodCall lst ->
                let lstValues = unwrapListExpr ([], lst)                
                qualifiedColumn (col, comparisonType lstValues)
            | Member col, Constant c -> 
                let lstValues = (c.Value :?> System.Collections.IEnumerable) |> Seq.cast<obj> |> Seq.toList
                qualifiedColumn (col, comparisonType lstValues)
            | _ -> notImpl()
        | MethodCall m when m.Method.Name = "like" ->
            match m.Arguments.[0], m.Arguments.[1] with
            | Member col, Constant c -> 
                let pattern = string c.Value
                qualifiedColumn (col, Like pattern)
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
                | Member col, Constant c
                | Constant c, Member col ->
                    // Handle regular column comparisons
                    let value = c.Value
                    let columnComparison = getColumnComparison(exp.NodeType, value)
                    qualifiedColumn (col, columnComparison)
                | Member col, MethodCall c when c.Type |> isOptionType ->
                    // Handle optional column comparisons
                    if c.Arguments.Count > 0 then 
                        match c.Arguments.[0] with
                        | Constant optVal -> 
                            let columnComparison = getColumnComparison(exp.NodeType, optVal.Value)
                            qualifiedColumn (col, columnComparison)
                        | _ -> 
                            notImpl()
                    else
                        let columnComparison = getColumnComparison(exp.NodeType, null)
                        qualifiedColumn (col, columnComparison)
                | _ ->
                    notImpl()
        | _ ->
            notImpl()

    visit (filter :> Expression)

/// Returns a fully qualified column name: {table}.{column}
let visitPropertySelector<'T, 'Prop> (propertySelector: Expression<Func<'T, 'Prop>>) =
    let rec visit (exp: Expression) : string =
        match exp with
        | Lambda x -> visit x.Body
        | Member m -> sprintf "%s.%s" m.Member.DeclaringType.Name m.Member.Name
        | _ -> notImpl()

    visit (propertySelector :> Expression)

let visitOrderBy<'T, 'Prop> (propertySelector: Expression<Func<'T, 'Prop>>, direction) =
    let propertyName = visitPropertySelector propertySelector
    OrderBy (propertyName, direction)

let visitJoin<'Left, 'Right> (joinOn: Expression<Func<'Left, 'Right, bool>>, joinType, schemaMaybe) =
    let rec visit (exp: Expression) : Join =
        match exp with
        | Lambda x -> visit x.Body
        | Binary x -> 
            match x.Left, x.Right with                
            | Member lt, Member rt ->
                match schemaMaybe with
                | Some schema -> 
                    let ltTbl = sprintf "%s.%s" schema typeof<'Left>.Name
                    let ltCol = sprintf "%s.%s" ltTbl lt.Member.Name
                    let rtTbl = sprintf "%s.%s" schema typeof<'Right>.Name
                    let rtCol = rt.Member.Name
                    joinType (rtTbl, rtCol, ltCol)
                | None -> 
                    let ltTbl = typeof<'Left>.Name
                    let ltCol = sprintf "%s.%s" ltTbl lt.Member.Name
                    let rtTbl = typeof<'Right>.Name
                    let rtCol = rt.Member.Name
                    joinType (rtTbl, rtCol, ltCol)
            | _ -> notImpl()
        | _ -> notImpl()

    visit (joinOn :> Expression)
