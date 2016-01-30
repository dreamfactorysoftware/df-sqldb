# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added

### Changed

### Fixed

## [0.2.0]
### Added
- Events now supported for Stored Procedures and Functions

### Changed
- **MAJOR** Updated code base to use OpenAPI (fka Swagger) Specification 2.0 from 1.2

### Fixed
- Virtual field check.

## [0.1.4] - 2015-12-21
### Changed
- Adhere to configurable max records returned limit for related records.

## [0.1.3] - 2015-12-18
### Fixed
- Filter parsing issues
- Consolidating schema caching classes, see df-core
- Rework filter handling of logical and comparison operators

## [0.1.2] - 2015-11-24
### Added
- Usage of df-core's new virtual foreign keys and aliasing for relationships.

### Changed
- Refactored relationship handling to use local or foreign services.

### Fixed
- Usage of SQL IN filtering syntax.

## [0.1.1] - 2015-11-20
### Added
- New virtual fields with DB function settings. Aggregate functions applied to fields.

### Fixed
- Fixed internal logic to use ColumnSchema from df-core instead of arrays.
- Fixed reported record creation issue.

## 0.1.0 - 2015-10-24
First official release working with the new [df-core](https://github.com/dreamfactorysoftware/df-core) library.

[Unreleased]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.2.0...HEAD
[0.2.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.4...0.2.0
[0.1.4]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.3...0.1.4
[0.1.3]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.2...0.1.3
[0.1.2]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.0...0.1.1
