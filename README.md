# OGA.Postgres.DAL
Data Access Layer for PostGres SQL

## Description
This library provides a Data Access Layer (DAL) and a class of tools for managing PostGres SQL databases, tables, and users.

## Installation
OGA.Postgres.DAL is available via NuGet:
* NuGet Official Releases: [![NuGet](https://img.shields.io/nuget/vpre/OGA.Postgres.DAL.svg?label=NuGet)](https://www.nuget.org/packages/OGA.Postgres.DAL)

## Dependencies
This library depends on:
* [NLog](https://github.com/NLog/NLog/)
* [Npgsql](https://www.nuget.org/packages/Npgsql)
* [OGA.Common.Lib](https://github.com/LeeWhite187/OGA.Common.Lib)
* [OGA.SharedKernel](https://github.com/LeeWhite187/OGA.SharedKernel)

## Building OGA.Postgres.DAL
This library is built with the new SDK-style projects.
It contains multiple projects, one for each of the following frameworks:
* NET 5
* NET 6
* NET 7

And, the output nuget package includes runtimes targets for:
* linux-any
* win-any

## Framework and Runtime Support
Currently, the nuget package of this library supports the framework versions and runtimes of applications that I maintain (see above).
If someone needs others (older or newer), let me know, and I'll add them to the build script.

## Visual Studio
This library is currently built using Visual Studio 2022 17.2.

## License
Please see the [License](LICENSE).

