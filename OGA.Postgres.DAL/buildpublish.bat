REM OGA PostGre SQL DAL Library

REM Build the library...
dotnet restore "./OGA.Postgres.DAL_NET5/OGA.Postgres.DAL_NET5.csproj"
dotnet build "./OGA.Postgres.DAL_NET5/OGA.Postgres.DAL_NET5.csproj" -c DebugLinux --runtime linux --no-self-contained

dotnet restore "./OGA.Postgres.DAL_NET5/OGA.Postgres.DAL_NET5.csproj"
dotnet build "./OGA.Postgres.DAL_NET5/OGA.Postgres.DAL_NET5.csproj" -c DebugWin --runtime win --no-self-contained

dotnet restore "./OGA.Postgres.DAL_NET6/OGA.Postgres.DAL_NET6.csproj"
dotnet build "./OGA.Postgres.DAL_NET6/OGA.Postgres.DAL_NET6.csproj" -c DebugLinux --runtime linux --no-self-contained

dotnet restore "./OGA.Postgres.DAL_NET6/OGA.Postgres.DAL_NET6.csproj"
dotnet build "./OGA.Postgres.DAL_NET6/OGA.Postgres.DAL_NET6.csproj" -c DebugWin --runtime win --no-self-contained

dotnet restore "./OGA.Postgres.DAL_NET7/OGA.Postgres.DAL_NET7.csproj"
dotnet build "./OGA.Postgres.DAL_NET7/OGA.Postgres.DAL_NET7.csproj" -c DebugLinux --runtime linux --no-self-contained

dotnet restore "./OGA.Postgres.DAL_NET7/OGA.Postgres.DAL_NET7.csproj"
dotnet build "./OGA.Postgres.DAL_NET7/OGA.Postgres.DAL_NET7.csproj" -c DebugWin --runtime win --no-self-contained

REM Create the composite nuget package file from built libraries...
D:\Programs\nuget\nuget.exe pack ./OGA.InfraBase.nuspec -IncludeReferencedProjects -symbols -SymbolPackageFormat snupkg -OutputDirectory ./Publish -Verbosity detailed

REM To publish nuget package...
dotnet nuget push -s https://buildtools.ogsofttech.com:8079/v3/index.json ".\Publish\OGA.Postgres.DAL.3.4.4.nupkg"
dotnet nuget push -s https://buildtools.ogsofttech.com:8079/v3/index.json ".\Publish\OGA.Postgres.DAL.3.4.4.snupkg"

TIMEOUT 10

ECHO "DONE"
