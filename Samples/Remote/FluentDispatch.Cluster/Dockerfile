FROM mcr.microsoft.com/dotnet/core/aspnet:3.1-buster-slim AS base
WORKDIR /app
EXPOSE 5432

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS build
WORKDIR /src
COPY ["FluentDispatch.PerformanceCounters/FluentDispatch.PerformanceCounters.csproj", "FluentDispatch.PerformanceCounters/"]
COPY ["FluentDispatch/FluentDispatch.csproj", "FluentDispatch/"]
COPY ["FluentDispatch.Monitoring/FluentDispatch.Monitoring.csproj", "FluentDispatch.Monitoring/"]
COPY ["FluentDispatch.Monitoring.InfluxDb/FluentDispatch.Monitoring.InfluxDb.csproj", "FluentDispatch.Monitoring.InfluxDb/"]
COPY ["FluentDispatch.Host/FluentDispatch.Host.csproj", "FluentDispatch.Host/"]
COPY ["FluentDispatch.Logging/FluentDispatch.Logging.csproj", "FluentDispatch.Logging/"]
COPY ["FluentDispatch.Logging.Serilog/FluentDispatch.Logging.Serilog.csproj", "FluentDispatch.Logging.Serilog/"]
COPY ["Samples/Remote/FluentDispatch.Cluster/FluentDispatch.Cluster.csproj", "Samples/Remote/FluentDispatch.Cluster/"]
COPY ["Samples/Remote/FluentDispatch.Contract/FluentDispatch.Contract.csproj", "Samples/Remote/FluentDispatch.Contract/"]
RUN dotnet restore "Samples/Remote/FluentDispatch.Cluster/FluentDispatch.Cluster.csproj" -s https://www.myget.org/F/appmetrics -s https://api.nuget.org/v3/index.json
COPY . .
WORKDIR "/src/Samples/Remote/FluentDispatch.Cluster"
RUN dotnet build "FluentDispatch.Cluster.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "FluentDispatch.Cluster.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "FluentDispatch.Cluster.dll"]