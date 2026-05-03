# bureau-test-dotnet Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-05-02

## Active Technologies

- C# / .NET 10 + Confluent.Kafka, Confluent.Kafka.DependencyInjection, CloudNative.CloudEvents, CloudNative.CloudEvents.Kafka, NSubstitute (001-kafka-observability-dashboard)

## Project Structure

```text
src/BureauObservability.sln
src/BureauObservability.Web/
src/BureauObservability.Tests/
specs/
```

## Commands

```bash
dotnet restore src/BureauObservability.sln   # install dependencies
dotnet build src/BureauObservability.sln     # compile
dotnet test src/BureauObservability.sln      # run tests (primary acceptance gate)
dotnet run --project src/BureauObservability.Web/BureauObservability.Web.csproj  # run dashboard
dotnet format src/BureauObservability.sln --verify-no-changes  # lint
```

## Code Style

C# / .NET 10: PascalCase for types and public members, camelCase for locals. Follow standard .NET conventions.

## Recent Changes

- 001-kafka-observability-dashboard: Added C# / .NET 10 + Confluent.Kafka (Kafka consumer), Confluent.Kafka.DependencyInjection (DI integration), NSubstitute (test mocking)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
