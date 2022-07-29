# Baobab

A pure Elixir implementation of [Bamboo](https://github.com/AljoschaMeyer/bamboo) append-only log.

It stores entries and identities in a filesystem spool.

## Configuration

The filesystem peristence is configured with

```
  config :baobab, spool_dir: "/tmp"
```
