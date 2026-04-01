#!/bin/sh
set -e
dotnet tool restore
dotnet paket restore
dotnet pack src/Dapper.FSharp/Dapper.FSharp.fsproj -c Release -o nupkg
