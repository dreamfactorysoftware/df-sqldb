# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
- DF-1275 Initial support for multi-column constraints

## [0.15.0] - 2017-12-26
### Added
- DF-1224 Added ability to set different default limits (max_records_returned) per service
- Added package discovery
- DF-1186 Added exceptions for missing data when generating relationships
- Add GraphQL support
### Changed
- DF-1150 Update copyright and support email
- Separate resources from resource handlers
- Used new getPrimaryKey method for better multi-key handling

## [0.14.0] - 2017-11-03
### Changed
- Change getNativeDateTimeFormat to handle column schema to detect detailed datetime format
- DF-1184 Limit schema object displayed fields when discovery is not complete
- Upgrade Swagger to OpenAPI 3.0 specification

## [0.13.1] - 2017-10-30
### Fixed
- Fix typo for stored functions

## [0.13.0] - 2017-09-18
### Added
- Support for HAS_ONE relationship in schema management and relationship handling
- DF-1060 Support for data retrieval (GET) caching and configuration
### Fixed
- DF-1165 Only squelch empty row set (2053) general MySQL error, propagate others
- Cleanup primary and unique key handling
- Correct foreign constraint discovery

## [0.12.0] - 2017-08-17
### Changed
- Reworked API doc usage and generation
- Removed direct use of Service model, using ServiceManager 
- Also cleaning up connection usage and correcting swagger
- Reworked schema interface for database services in order to better control caching
- Set config-based cache prefix
- Fix caching for empty query results

## [0.11.0] - 2017-07-27
### Changed
- Separating base schema from SQL schema
- Support PDO constants in config options
### Fixed
- Datetime settings handling
- Fix boolean filter value

## [0.10.0] - 2017-06-05
### Changed
- Cleanup - removal of php-utils dependency
- No need for count on single id requests

## [0.9.1] - 2017-04-25
### Fixed
- Fix upsert response

## [0.9.0] - 2017-04-21
### Added
- DF-811 Add support for upsert
### Changed
- Use new service config handling for database configuration
### Fixed
- DF-1033 Correct datetime config option usage
- DF-1008 Correct inconsistent behavior regarding selected fields and related data

## [0.8.2] - 2017-03-29
### Fixed
- Broken usage of group "GROUP BY" option

## [0.8.1] - 2017-03-20
### Fixed
- Using '*' for related parameter getting overwritten

## [0.8.0] - 2017-03-03
### Changed
- Batch requests now report errors consistently using BatchException

## [0.7.1] - 2017-01-25
### Added
- Added DatabaseSeeder class to seed 'db' service

## [0.7.0] - 2017-01-16
### Changed
- Refactor for separate database service repo, see df-database
- DF-814 Database function support across all fields, not just virtual
- Prefer sqlsrv driver over dblib
- Clean out use of MERGE verb, handled at router/controller level
- Cleanup schema management issues

### Fixed
- Fix use of special words in table names for SQLite

## [0.6.0] - 2016-11-17
### Added
- Added _field and _related resources to _schema/<table> API paths to help support virtual relationships

### Changed
- Virtual relationships rework to support all relationship types
- DB base class changes to support field configuration across all database types.
- Updated API Docs to support new database schema API paths and resources

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

[Unreleased]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.15.0...HEAD
[0.15.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.14.0...0.15.0
[0.14.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.13.1...0.14.0
[0.13.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.13.0...0.13.1
[0.13.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.12.0...0.13.0
[0.12.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.11.0...0.12.0
[0.11.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.10.0...0.11.0
[0.10.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.9.1...0.10.0
[0.9.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.9.0...0.9.1
[0.9.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.8.2...0.9.0
[0.8.2]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.8.1...0.8.2
[0.8.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.8.0...0.8.1
[0.8.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.7.1...0.8.0
[0.7.1]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.7.0...0.7.1
[0.7.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.6.0...0.7.0
[0.6.0]: https://github.com/dreamfactorysoftware/df-sqldb/compare/0.5.0...0.6.0
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
