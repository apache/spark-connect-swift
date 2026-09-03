//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

// MARK: - URL functions

/// Extracts a part from a URL.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: The part to extract from the URL, e.g. `HOST`, `PATH`, `QUERY`, `REF`,
///     `PROTOCOL`, `FILE`, `AUTHORITY`, or `USERINFO`.
/// - Returns: A ``Column`` that evaluates to a string.
public func parse_url(_ url: Column, _ partToExtract: String) -> Column {
  return parse_url(url, lit(partToExtract))
}

/// Extracts a part from a URL.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: A ``Column`` that evaluates to the part to extract from the URL,
///     e.g. `HOST`, `PATH`, `QUERY`, `REF`, `PROTOCOL`, `FILE`, `AUTHORITY`, or `USERINFO`.
/// - Returns: A ``Column`` that evaluates to a string.
public func parse_url(_ url: Column, _ partToExtract: Column) -> Column {
  return fn("parse_url", url, partToExtract)
}

/// Extracts the value of a query parameter from a URL.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: The part to extract from the URL. Only `QUERY` uses `key`.
///   - key: The key of a query parameter in the URL.
/// - Returns: A ``Column`` that evaluates to a string.
public func parse_url(_ url: Column, _ partToExtract: String, _ key: String) -> Column {
  return parse_url(url, lit(partToExtract), lit(key))
}

/// Extracts the value of a query parameter from a URL.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: A ``Column`` that evaluates to the part to extract from the URL.
///     Only `QUERY` uses `key`.
///   - key: A ``Column`` that evaluates to the key of a query parameter in the URL.
/// - Returns: A ``Column`` that evaluates to a string.
public func parse_url(_ url: Column, _ partToExtract: Column, _ key: Column) -> Column {
  return fn("parse_url", url, partToExtract, key)
}

/// Extracts a part from a URL. This is a special version of ``parse_url(_:_:)`` that returns
/// a `NULL` value instead of raising an error if the URL is invalid.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: The part to extract from the URL, e.g. `HOST`, `PATH`, `QUERY`, `REF`,
///     `PROTOCOL`, `FILE`, `AUTHORITY`, or `USERINFO`.
/// - Returns: A ``Column`` that evaluates to a string.
public func try_parse_url(_ url: Column, _ partToExtract: String) -> Column {
  return try_parse_url(url, lit(partToExtract))
}

/// Extracts a part from a URL. This is a special version of ``parse_url(_:_:)`` that returns
/// a `NULL` value instead of raising an error if the URL is invalid.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: A ``Column`` that evaluates to the part to extract from the URL,
///     e.g. `HOST`, `PATH`, `QUERY`, `REF`, `PROTOCOL`, `FILE`, `AUTHORITY`, or `USERINFO`.
/// - Returns: A ``Column`` that evaluates to a string.
public func try_parse_url(_ url: Column, _ partToExtract: Column) -> Column {
  return fn("try_parse_url", url, partToExtract)
}

/// Extracts the value of a query parameter from a URL. This is a special version of
/// ``parse_url(_:_:_:)`` that returns a `NULL` value instead of raising an error if the URL
/// is invalid.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: The part to extract from the URL. Only `QUERY` uses `key`.
///   - key: The key of a query parameter in the URL.
/// - Returns: A ``Column`` that evaluates to a string.
public func try_parse_url(_ url: Column, _ partToExtract: String, _ key: String) -> Column {
  return try_parse_url(url, lit(partToExtract), lit(key))
}

/// Extracts the value of a query parameter from a URL. This is a special version of
/// ``parse_url(_:_:_:)`` that returns a `NULL` value instead of raising an error if the URL
/// is invalid.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - url: A ``Column`` that evaluates to a URL string.
///   - partToExtract: A ``Column`` that evaluates to the part to extract from the URL.
///     Only `QUERY` uses `key`.
///   - key: A ``Column`` that evaluates to the key of a query parameter in the URL.
/// - Returns: A ``Column`` that evaluates to a string.
public func try_parse_url(_ url: Column, _ partToExtract: Column, _ key: Column) -> Column {
  return fn("try_parse_url", url, partToExtract, key)
}

/// Decodes a `str` in 'application/x-www-form-urlencoded' format using a specific encoding
/// scheme.
/// - Parameter str: A ``Column`` that evaluates to a URL-encoded string.
/// - Returns: A ``Column`` that evaluates to a string.
public func url_decode(_ str: Column) -> Column {
  return fn("url_decode", str)
}

/// Translates a string into 'application/x-www-form-urlencoded' format using a specific
/// encoding scheme.
/// - Parameter str: A ``Column`` that evaluates to a string to encode.
/// - Returns: A ``Column`` that evaluates to a string.
public func url_encode(_ str: Column) -> Column {
  return fn("url_encode", str)
}

/// Decodes a `str` in 'application/x-www-form-urlencoded' format using a specific encoding
/// scheme. This is a special version of ``url_decode(_:)`` that returns a `NULL` value instead
/// of raising an error if the decoding cannot be performed.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameter str: A ``Column`` that evaluates to a URL-encoded string.
/// - Returns: A ``Column`` that evaluates to a string.
public func try_url_decode(_ str: Column) -> Column {
  return fn("try_url_decode", str)
}
