# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## UNRELEASED

# 0.0.11 - 2026-02-20

* Fixes for caching working correctly and ensuring we don't recalculate / re-parse Avro schemas

# 0.0.10 - 2026-02-11

* Fix: Do not send `schemaType` or `references` if schema type is `AVRO`, for backwards compatibility with older schema registries.

# 0.0.8 - 2026-02-04

* Support passing a `schema_store` into the `Avro` schema backend.
* Move schema backend methods from class-level to instance-level and require instantiation.

# 0.0.7 - 2026-01-05

* Switch to using `SchemaRegistry::Client` instead of bare `SchemaRegistry`.
* Throw correct error when Avro schema is not found.

# 0.0.6 - 2026-01-02

* Initial release.
