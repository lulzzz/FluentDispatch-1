@ECHO OFF

:: This CMD script setup all Docker containers.

TITLE FluentDispatch Docker Setup

ECHO Please wait... Creating HTTPS certificate.
dotnet dev-certs https --clean
dotnet dev-certs https -ep %USERPROFILE%\.aspnet\https\fluentdispatch.pfx -p fluentdispatch
dotnet dev-certs https --trust

ECHO Please wait... Launching containers.
docker-compose -f docker-compose.yml -f docker-compose.windows.yml up