# Kvasir

Opinionated Kafka library.

## Installation

The package can be installed by adding `kvasir`
to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kvasir, "~> 0.0.1"},
  ]
end
```

The docs can be found at [https://hexdocs.pm/kvasir](https://hexdocs.pm/kvasir).

## Optional Dependencies
### Kvasir Agent

[Kvasir Agent](https://github.com/IanLuites/kvasir_agent)
extension to allow for aggregated state processes.

### KSQL

[KSQL](https://github.com/IanLuites/ksql)
extension to allow for KSQL queries and commands.

Also replaces the streams with `KSQL` `print` queries.
