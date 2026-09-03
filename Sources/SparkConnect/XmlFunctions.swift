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

// MARK: - XML functions

/// Parses a column containing an XML string into a struct with the given schema.
/// Returns `NULL`, in the case of an unparseable string.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an XML string.
///   - schema: A DDL schema string, e.g. `a INT, b STRING`.
///   - options: Options to control how the XML is parsed. It accepts the same options as
///     the XML data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_xml(
  _ col: Column, _ schema: String, _ options: [String: String] = [:]
) -> Column {
  return fn("from_xml", options: options, col, lit(schema))
}

/// Parses a column containing an XML string into a struct with the given schema.
/// Returns `NULL`, in the case of an unparseable string.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an XML string.
///   - schema: A ``StructType`` schema.
///   - options: Options to control how the XML is parsed. It accepts the same options as
///     the XML data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_xml(
  _ col: Column, _ schema: StructType, _ options: [String: String] = [:]
) -> Column {
  return from_xml(col, schema.toDDL, options)
}

/// Parses a column containing an XML string into a struct with the given schema.
/// Returns `NULL`, in the case of an unparseable string.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to an XML string.
///   - schema: A ``Column`` that evaluates to a constant schema string,
///     e.g. a `schema_of_xml` result.
///   - options: Options to control how the XML is parsed. It accepts the same options as
///     the XML data source.
/// - Returns: A ``Column`` of the type given by the schema.
public func from_xml(
  _ col: Column, _ schema: Column, _ options: [String: String] = [:]
) -> Column {
  return fn("from_xml", options: options, col, schema)
}

/// Parses an XML string and infers its schema in DDL format.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - xml: An XML string.
///   - options: Options to control how the XML is parsed. It accepts the same options as
///     the XML data source.
/// - Returns: A ``Column`` that evaluates to a schema string in DDL format.
public func schema_of_xml(_ xml: String, _ options: [String: String] = [:]) -> Column {
  return schema_of_xml(lit(xml), options)
}

/// Parses an XML string and infers its schema in DDL format.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to a constant XML string.
///   - options: Options to control how the XML is parsed. It accepts the same options as
///     the XML data source.
/// - Returns: A ``Column`` that evaluates to a schema string in DDL format.
public func schema_of_xml(_ xml: Column, _ options: [String: String] = [:]) -> Column {
  return fn("schema_of_xml", options: options, xml)
}

/// Converts a column containing a struct into an XML string.
/// Throws an exception, in the case of an unsupported type.
/// This requires Apache Spark 4.0.0 or later.
/// - Parameters:
///   - col: A ``Column`` that evaluates to a struct.
///   - options: Options to control how the column is converted. It accepts the same options as
///     the XML data source.
/// - Returns: A ``Column`` that evaluates to a string.
public func to_xml(_ col: Column, _ options: [String: String] = [:]) -> Column {
  return fn("to_xml", options: options, col)
}

/// Returns a string array of values within the nodes of `xml` that match the XPath expression.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to an array of strings.
public func xpath(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath", xml, path)
}

/// Returns `true` if the XPath expression evaluates to `true`, or if a matching node is found.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to a boolean.
public func xpath_boolean(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_boolean", xml, path)
}

/// Returns a double value, the value zero if no match is found, or `NaN` if a match is found
/// but the value is non-numeric.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to a double.
public func xpath_double(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_double", xml, path)
}

/// Returns a float value, the value zero if no match is found, or `NaN` if a match is found
/// but the value is non-numeric.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to a float.
public func xpath_float(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_float", xml, path)
}

/// Returns an integer value, the value zero if no match is found, or a match is found but the
/// value is non-numeric.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to an integer.
public func xpath_int(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_int", xml, path)
}

/// Returns a long integer value, the value zero if no match is found, or a match is found but
/// the value is non-numeric.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to a long.
public func xpath_long(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_long", xml, path)
}

/// Returns a double value, the value zero if no match is found, or `NaN` if a match is found
/// but the value is non-numeric. This is an alias of ``xpath_double(_:_:)``.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to a double.
public func xpath_number(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_number", xml, path)
}

/// Returns a short integer value, the value zero if no match is found, or a match is found but
/// the value is non-numeric.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to a short.
public func xpath_short(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_short", xml, path)
}

/// Returns the text contents of the first XML node that matches the XPath expression.
/// - Parameters:
///   - xml: A ``Column`` that evaluates to an XML string.
///   - path: A ``Column`` that evaluates to a constant XPath expression.
/// - Returns: A ``Column`` that evaluates to a string.
public func xpath_string(_ xml: Column, _ path: Column) -> Column {
  return fn("xpath_string", xml, path)
}
