module internal Dapper.FSharp.SQLite.LinqExpressionVisitors

open System.Linq.Expressions
open System
open System.Reflection

let notImpl() = raise (NotImplementedException())
let notImplMsg msg = raise (NotImplementedException msg)

[<AutoOpen>]
module VisitorPatterns =

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

[<AutoOpen>]
module SqlPatterns = 

    let (|Not|_|) (exp: Expression) = 
        match exp.NodeType with
        | ExpressionType.Not -> Some (exp :?> UnaryExpression)
        | _ -> None

    let (|BinaryAnd|_|) (exp: Expression) =
        match exp.NodeType with
        | ExpressionType.And
        | ExpressionType.AndAlso -> Some (exp :?> BinaryExpression)
        | _ -> None

    let (|BinaryOr|_|) (exp: Expression) =
        match exp.NodeType with
        | ExpressionType.Or
        | ExpressionType.OrElse -> Some (exp :?> BinaryExpression)
        | _ -> None

    let (|BinaryCompare|_|) (exp: Expression) =
        match exp.NodeType with
        | ExpressionType.Equal
        | ExpressionType.NotEqual
        | ExpressionType.GreaterThan
        | ExpressionType.GreaterThanOrEqual
        | ExpressionType.LessThan
        | ExpressionType.LessThanOrEqual -> Some (exp :?> BinaryExpression)
        | _ -> None

    let isOptionType (t: Type) = 
        t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Option<_>>

    /// A property member, a property wrapped in 'Some', or an option 'Value'.
    let (|Property|_|) (exp: Expression) =
        let tryGetMember(x: Expression) = 
            match x with
            | Member m when m.Expression.NodeType = ExpressionType.Parameter -> 
                Some m.Member
            | MethodCall opt when opt.Type |> isOptionType ->        
                if opt.Arguments.Count > 0 then
                    // Option.Some
                    match opt.Arguments.[0] with
                    | Member m -> Some m.Member
                    | _ -> None
                else None
            | _ -> None

        match exp with
        | Member m when m.Member.DeclaringType <> null && m.Member.DeclaringType |> isOptionType -> 
            // Handles option '.Value'
            tryGetMember m.Expression
        | _ -> 
            tryGetMember exp

    /// A constant value or an optional constant value
    let (|Value|_|) (exp: Expression) =
        match exp with
        | New n when n.Type.Name = "Guid" -> 
            let value = (n.Arguments.[0] :?> ConstantExpression).Value :?> string
            Some (Guid(value) |> box)
        | Member m when m.Expression.NodeType = ExpressionType.Constant -> 
            // Extract constant value from property (probably a record property)
            // NOTE: This currently does not unwind nested properties! 
            // NOTE: This uses reflection; it is more performant for user to manually unwrap and pass in constant.
            let parentObject = (m.Expression :?> ConstantExpression).Value
            match m.Member.MemberType with
            | MemberTypes.Field -> (m.Member :?> FieldInfo).GetValue(parentObject) |> Some
            | MemberTypes.Property -> (m.Member :?> PropertyInfo).GetValue(parentObject) |> Some
            | _ -> notImplMsg(sprintf "Unable to unwrap where value for '%s'" m.Member.Name)
        | Member m when m.Expression.NodeType = ExpressionType.MemberAccess -> 
            // Extract constant value from nested object/properties
            notImplMsg "Nested property value extraction is not supported in 'where' statements. Try manually unwrapping and passing in the value."
        | Constant c -> Some c.Value
        | MethodCall opt when opt.Type |> isOptionType ->        
            if opt.Arguments.Count > 0 then
                // Option.Some
                match opt.Arguments.[0] with
                | Constant c -> Some c.Value
                | _ -> None
            else
                // Option.None
                Some null
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
        | Not x -> 
            let operand = visit x.Operand
            Unary (Not, operand)
        | MethodCall m when m.Method.Name = "Invoke" ->
            // Handle tuples
            visit m.Object
        | MethodCall m when List.contains m.Method.Name [ "isIn"; "isNotIn" ] ->
            let comparisonType = m.Method.Name |> function "isIn" -> In | _ -> NotIn
            match m.Arguments.[0], m.Arguments.[1] with
            | Property p, MethodCall lst ->
                let lstValues = unwrapListExpr ([], lst)                
                Column (qualifyColumn p, comparisonType lstValues)
            | Property p, Value value -> 
                let lstValues = (value :?> System.Collections.IEnumerable) |> Seq.cast<obj> |> Seq.toList
                Column (qualifyColumn p, comparisonType lstValues)
            | _ -> notImpl()
        | MethodCall m when List.contains m.Method.Name [ "like"; "notLike" ] ->
            match m.Arguments.[0], m.Arguments.[1] with
            | Property p, Value value -> 
                let pattern = string value
                match m.Method.Name with
                | "like" -> Column ((qualifyColumn p), (Like pattern))
                | _ -> Column ((qualifyColumn p), (NotLike pattern))
            | _ -> notImpl()
        | MethodCall m when m.Method.Name = "isNullValue" || m.Method.Name = "isNotNullValue" ->
            match m.Arguments.[0] with
            | Property p -> 
                if m.Method.Name = "isNullValue" 
                then Column (qualifyColumn p, ColumnComparison.IsNull)
                else Column (qualifyColumn p, ColumnComparison.IsNotNull)
            | _ -> notImpl()
        | BinaryAnd x ->
            let lt = visit x.Left
            let rt = visit x.Right
            Binary (lt, And, rt)
        | BinaryOr x -> 
            let lt = visit x.Left
            let rt = visit x.Right
            Binary (lt, Or, rt)
        | BinaryCompare x ->
            match x.Left, x.Right with            
            | Property p1, Property p2 ->
                // Handle col to col comparisons
                let lt = qualifyColumn p1
                let cp = getComparison exp.NodeType
                let rt = qualifyColumn p2
                Expr (sprintf "%s %s %s" lt cp rt)
            | Property p, Value value
            | Value value, Property p ->
                // Handle column to value comparisons
                let columnComparison = getColumnComparison(exp.NodeType, value)
                Column (qualifyColumn p, columnComparison)
            | Value v1, Value v2 ->
                // Not implemented because I didn't want to embed logic to properly format strings, dates, etc.
                // This can be easily added later if it is implemented in Dapper.FSharp.
                notImplMsg("Value to value comparisons are not currently supported. Ex: where (1 = 1)")
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

type JoinInfo = 
    | MI of MemberInfo
    | Const of obj

/// Returns one or more column members
let visitJoin<'T, 'Prop> (propertySelector: Expression<Func<'T, 'Prop>>) =
    let rec visit (exp: Expression) : JoinInfo list =
        match exp with
        | Lambda x -> visit x.Body
        | MethodCall m when m.Method.Name = "Invoke" ->
            // Handle tuples
            visit m.Object
        | New n -> 
            // Handle groupBy that returns a tuple of multiple columns
            n.Arguments |> Seq.map visit |> Seq.toList |> List.collect id
        | Member m -> 
            if m.Member.DeclaringType |> isOptionType
            then visit m.Expression
            else [ MI m.Member ]
        | Property mi -> [ MI mi ]
        | Constant c -> 
            [ Const c.Value ]
        | _ -> notImpl()

    visit (propertySelector :> Expression)

/// Returns a column member
let visitPropertySelector<'T, 'Prop> (propertySelector: Expression<Func<'T, 'Prop>>) =
    let rec visit (exp: Expression) : MemberInfo =
        match exp with
        | Lambda x -> visit x.Body
        | MethodCall m when m.Method.Name = "Invoke" ->
            // Handle tuples
            visit m.Object
        | Member m -> 
            if m.Member.DeclaringType |> isOptionType
            then visit m.Expression
            else m.Member
        | Property mi -> mi
        | _ -> notImpl()

    visit (propertySelector :> Expression)
