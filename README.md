# OGA.Postgres.DAL
Data Access Layer for PostGres SQL

## Description
This library provides a Data Access Layer (DAL) and a class of tools for managing PostGres SQL databases, tables, and users.

Postgres_DAL
   Wraps the ceremony of using NpgSQL, to perform the following types of actions:
    - Tabular and Scalar Queries
    - Non Queries
      for sql commands, such as: database creations, role grants, table drops, etc...
    - Bulk Binary Importing
      Syntactical sugar for the NpgsqlBinaryImporter, which provides multi-row inserts without the need for string sanitization
    - Stored Procedures calls
      With RC return, and with/without Terminal Select

* Postgres_Tools
  This class includes lots of methods for host, database, and user management.
  - Host Management:
    - Get_DataDirectory() - identifies the filesystem path where databases are stored
    - Get_Database_FolderPath() - locates the specific folder path for a database

- Database Management:
    - Create_Database()
    - Is_Database_Present()
    - Drop_Database()
    - GetDatabaseOwner()
    - ChangeDatabaseOwner()
    - Backup_Database() - Not yet implemented.
    - Restore_Database() - Not yet implemented.
    - Get_DatabaseSize()

- User Management:
    - GetUserList()
    - Does_Login_Exist()
    - CreateUser()
    - ChangeUserPassword()
    - DeleteUser()

- Permissions Management:
    - GrantSuperUser()
    - IsSuperUser()
    - DenySuperUser()

    - GrantDBCreate()
    - HasDBCreate()
    - DenyDBCreate()

    - GrantCreateRole()
    - HasCreateRole()
    - DenyCreateRole()

    - GrantAllforUserOnDatabase()
    - GrantAllforUserOnTable()
    - GetTablePrivilegesforUser()
    - SetTablePrivilegesforUser()

- Table Management:
    - Create_Table()
    - DoesTableExist()
    - Drop_Table()
    - Get_TableList_forDatabase()
    - Get_PrimaryKeyConstraints_forTable() - key_column, constraint_name, position_ordinal
    - Get_ColumnList_forTable() - list of column names in a table.
    - Get_ColumnInfo_forTable() - columnname, datatype, length, isnullable, etc...
    - Get_TableSize()
    - Get_RowCount_for_Tables()


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
