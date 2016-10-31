# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
- Added _field and _related resources to _schema/<table> API paths to help support virtual relationships

### Changed
- Updated API Docs to support new database schema API paths and resources

### Fixed

## [0.5.0] - 2016-09-30
### Changed
- DF-826 Update Config models with df-core changes for encryption and protection.
- DF-873 Improve related data queries by pushing full dataset down to per relationship handling.

## [0.4.0] - 2016-08-21
### Changed
- DF-681 Event firing changes for resources.
- DF-607 Making service docs always viewable, even auto-generated ones.
### Fixed
- Wrapper not affecting stored procedure output when no output parameters are present.

## [0.3.2] - 2016-07-08
### Added
- DF-636 Adding ability using 'ids' parameter to return the schema of a stored procedure or function

### Changed
- DF-799 Exchange expression for laravel's DB::raw() usage

## [0.3.1] - 2016-06-02
### Fixed
- Group By ("group") query param now allows multiple fields or a function call.

## [0.3.0] - 2016-05-27
### Changed
- Moved seeding functionality to service provider to adhere to df-core changes.
- Moved SQL Server, SQL Anywhere, and Oracle support to their own repos.
- Database config now supports individual options easier, help text updated.

### Fixed
- Bug fixes for schema extensions.

## [0.2.2] - 2016-04-22
### Changed
- Major change for df-core rework of database connections.

### Fixed
- Look for params as well as payload 
- Insert scenario when id provided for record
- Quote filter value in case the other service is NoSQL 

## [0.2.1] - 2016-03-08
### Added
- Filter support for contains, starts with and ends with.
- Lookup support for stored procedures and functions parameters.

### Fixed
- Better catch for negation in filter support.
- Updates for Swagger spec.

## [0.2.0] - 2016-01-29
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

[Unreleased]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.5.0...HEAD
[0.5.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.4.0...0.5.0
[0.4.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.3.2...0.4.0
[0.3.2]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.3.1...0.3.2
[0.3.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.2.2...0.3.0
[0.2.2]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.4...0.2.0
[0.1.4]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.3...0.1.4
[0.1.3]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.2...0.1.3
[0.1.2]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.1.0...0.1.1
