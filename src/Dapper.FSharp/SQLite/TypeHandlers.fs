[<RequireQualifiedAccess>]
[<System.Obsolete("""This module is deprecated and will be removed in next version.
Please use function from a database vendor-scoped namespace.
Example: Dapper.FSharp.SQLite.OptionTypes.register()""")>]
module Dapper.FSharp.SQLite.TypeHandlers

[<System.Obsolete("""This function is deprecated and will be removed in next version.
Please use function from a database vendor-scoped namespace.
Example: Dapper.FSharp.SQLite.OptionTypes.register()""")>]
let addSQLiteTypeHandlers() =
    Dapper.FSharp.SQLite.OptionTypes.register()
