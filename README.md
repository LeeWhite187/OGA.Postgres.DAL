# OGA.Postgres.DAL
Data Access Layer for PostGres SQL

## Description
This library provides a Data Access Layer (DAL) and a class of tools for managing PostGres SQL databases, tables, and users.

#### Postgres_DAL
Wraps the ceremony of using NpgSQL, to perform the following types of actions:
- Tabular and Scalar Queries
- Non Queries\
   For sql commands, such as: database creations, role grants, table drops, etc...
- Bulk Binary Importing\
   Syntactical sugar for the NpgsqlBinaryImporter, which provides multi-row inserts without the need for string sanitization
- Stored Procedures calls\
   With RC return, and with/without Terminal Select

#### DatabaseLayout_Tool
Abstracts database/table structure management of a PostgreSQL database, into a serializable layout of POCOs.\
This allows for database management in terms of JSON-serializable layout objects, instead of SQL scripts.\
Using the layout abstraction of this class, allows the following use cases:
- Create a database from a layout
- Create a layout from a database
- Serialize a layout (of a database) to and from json
- Version a database layout in git, as json
- Reconcile a database and layout, generating a list of differences
- Reconcile differences between database instances... across environments, such as: dev/test/prod
- Verify a database is compatible with a starting service, by performing a verification check with a json-serialized layout.

#### Postgres_Tools
This class includes lots of methods for management of hosts, databases, tables, users, and permissions, allowing the developer to work in terms of method calls, without SQL scripts.
- Host Management:
   - Get_DataDirectory()\
     Identifies the filesystem path where databases are stored
   - Get_Database_FolderPath()\
     Locates the specific folder path for a database

- Database Management:
    - Create_Database()
    - Is_Database_Present()
    - Drop_Database()
    - GetDatabaseOwner()
    - ChangeDatabaseOwner()
    - Backup_Database() - Not yet implemented.
    - Restore_Database() - Not yet implemented.
    - Get_DatabaseSize()

- Table Management:
    - Create_Table()
    - DoesTableExist()
    - Drop_Table()
    - Get_TableList_forDatabase()
    - Get_PrimaryKeyConstraints_forTable()\
      Key_column, constraint_name, position_ordinal
    - Get_ColumnList_forTable()\
      List of column names in a table.
    - Get_ColumnInfo_forTable()\
      Columnname, datatype, length, isnullable, etc...
    - Get_TableSize()
    - Get_RowCount_for_Tables()

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
Currently, the nuget package of this library supports the framework versions and runtimes of applications that I maintain (see above).\
If someone needs others (older or newer), let me know, and I'll add them to the build script.

## Visual Studio
This library is currently built using Visual Studio 2022 17.2.

## License
Please see the [License](LICENSE).
