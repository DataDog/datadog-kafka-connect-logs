Changelog
=========

# 1.1.2 / 2024-04-16

### Changes

* Removed call to `disconnect` on `HttpURLConnection` to encourage connection reuse.
* Revamped debug and trace level logging to make debug logging more useful
* Added `User-Agent` header to requests

# 1.1.1 / 2022-06-07

### Changes
* Fix CVE-2022-25647 by upgrading the version of `com.google.code.gson` to `2.8.9`.

# 1.1.0 / 2022-01-17

### Changes
* Add datadog url endpoint configurable from parameters
* Add datadog site configurable from parameters
* Use API V2 endpoint

# 1.0.3 / 2021-12-18

### Changes
* Remove log4j2 dependency

# 1.0.2 / 2021-12-15

### Changes
* Update log4j2 dependency to `2.16.0`

# 1.0.1 / 2021-12-11

### Changes
* Update log4j2 dependency to `2.15.0`

# 1.0.0 / 2020-10-16

Initial release
