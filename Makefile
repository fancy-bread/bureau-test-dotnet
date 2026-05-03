.PHONY: ci restore lint build test

ci: restore lint build test

restore:
	dotnet restore src/

lint:
	dotnet format src/ --verify-no-changes

build:
	dotnet build src/ --no-restore

test:
	dotnet test src/ --no-build
