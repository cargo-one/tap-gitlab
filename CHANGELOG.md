# Changelog

All notable changes to this project will be documented in this file.

## [1.0.4] - 2025-07-09

### Fixed
- **Timestamp Handling**: Fixed format_timestamp function to handle None values properly
- **Schema Validation**: Prevented TypeError when merge request fields contain null timestamp values
- **Data Processing**: Improved robustness of date-time field processing for nullable timestamp fields

### Technical Improvements
- Added null check in format_timestamp pre-hook function
- Enhanced error handling for optional timestamp fields in merge requests
- Improved data transformation resilience for incomplete API responses

## [1.0.3] - 2025-07-09

### Fixed
- **Log Size Issue**: Further reduced log verbosity to prevent 1.04M log entries exceeding 256K limit
- **Third-party Logging**: Improved filtering of Google Cloud and connection pool debug logs
- **State Initialization**: Fixed missing stream initialization for new code review metrics

### Technical Improvements
- Enhanced logging configuration to reduce stderr output
- Improved state management for incremental sync of new streams
- Added more aggressive log filtering for production environments

## [1.0.2] - 2025-07-08

### Fixed
- **Deployment Schema**: Fixed schema validation errors by making nested objects nullable
- **Missing Projects**: Added error handling to gracefully skip 404 projects instead of crashing
- **Error Resilience**: Improved job stability when encountering missing or inaccessible projects
- **Log Size**: Reduced log verbosity to prevent oversized logs (>256K limit)

### Technical Improvements
- Enhanced error handling in `sync_project()` function
- Made deployments schema more flexible for incomplete API responses
- Changed API request logging from INFO to DEBUG level
- Reduced metrics calculation and deployment error logging verbosity

## [1.0.1] - 2025-01-08

### Fixed
- **API Parameter Issues**: Fixed malformed query parameters causing 400 errors
- **Deployments API**: Removed problematic `updated_after` filter for deployments endpoint
- **Error Handling**: Improved error handling for 403 Forbidden and API errors
- **URL Parsing**: Fixed URL parameter parsing to prevent empty parameter names
- **Graceful Degradation**: Added graceful handling when deployment access is forbidden

### Technical Improvements
- Enhanced URL generation with proper parameter handling
- Improved pagination parameter merging
- Added comprehensive error logging for debugging

## [1.0.0] - 2025-01-08

### Added
- **New Streams**: Added support for merge requests, discussions, and notes extraction
- **Code Review Metrics**: Comprehensive code review analytics including:
  - Review Ratio: Track reviewer participation relative to MR creation
  - Review Speed: Measure time to first review and approval
  - Review Depth: Count comments per lines of code changed
  - MR Quality: Track commits made after initial review feedback
  - Staging Deployment: Monitor pipeline execution before merge
- **Computed Fields**: 
  - `first_review_at` - Timestamp of first review comment
  - `first_approval_at` - Timestamp of first approval
  - `commits_after_first_review` - Count of commits after review feedback
  - `has_staging_deployment` - Boolean flag for staging pipeline execution
  - `staging_deployment_at` - Timestamp of successful staging deployment
- **Enhanced Schemas**: New JSON schemas for merge_requests, discussions, and notes
- **Pipeline Integration**: Enhanced pipeline tracking with job-level details

### Changed
- **BREAKING**: Upgraded singer-python from 5.0.4 to 6.1.1
- **BREAKING**: Updated backoff dependency from 1.3.2 to 2.2.1+
- **Dependencies**: Updated requests to 2.32.0+ for security and Python 3.12 compatibility
- **Python Support**: Added explicit Python 3.12 support and compatibility
- **Schema Package**: Updated setup.py to include all schema files
- **API Coverage**: Extended GitLab API integration to include MR-specific endpoints

### Technical Improvements
- Added comprehensive error handling for metrics calculation
- Improved pagination handling for large datasets
- Enhanced state management for incremental sync of new streams
- Added computed metrics calculation during extraction
- Implemented proper URL generation for nested API endpoints

### Migration Notes
- **State Reset Required**: New streams require state initialization (handled automatically)
- **Dependency Update**: Ensure singer-python 6.1.1 compatibility in deployment environment
- **BigQuery Schema**: New tables will be created for merge_requests, discussions, and notes
- **Incremental Sync**: First run will perform full historical extraction for new streams

## [0.5.1] - Previous Version
- Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## [0.5.0] - Previous Version
- Added support for groups and group milestones [#9](https://github.com/singer-io/tap-gitlab/pull/9)
