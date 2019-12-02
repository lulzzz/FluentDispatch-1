#!/bin/sh
echo "Please wait... Creating HTTPS certificate."
dotnet dev-certs https --clean
dotnet dev-certs https -ep ${HOME}/.aspnet/https/fluentdispatch.pfx -p fluentdispatch
dotnet dev-certs https --trust

echo "Please wait... Launching containers."
docker-compose -f docker-compose.yml -f docker-compose.mac-linux.yml up