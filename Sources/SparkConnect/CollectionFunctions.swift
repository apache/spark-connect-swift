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

// MARK: - Collection functions

/// Creates a new array column. The input columns must all have the same data type.
/// - Parameter cols: ``Column``s to be combined into an array.
/// - Returns: A ``Column``.
public func array(_ cols: Column...) -> Column {
  return fn("array", cols)
}

/// Appends the element to the end of the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - element: An element ``Column`` to be appended.
/// - Returns: A ``Column``.
public func array_append(_ column: Column, _ element: Column) -> Column {
  return fn("array_append", column, element)
}

/// Appends the element to the end of the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - element: A literal element to be appended.
/// - Returns: A ``Column``.
public func array_append(_ column: Column, _ element: some SparkLiteral) -> Column {
  return array_append(column, element.toLiteralColumn)
}

/// Removes null values from the array.
/// - Parameter column: An array ``Column``.
/// - Returns: A ``Column``.
public func array_compact(_ column: Column) -> Column {
  return fn("array_compact", column)
}

/// Returns null if the array is null, true if the array contains `value`, and false otherwise.
/// - Parameters:
///   - column: An array ``Column``.
///   - value: A value ``Column`` to check.
/// - Returns: A ``Column``.
public func array_contains(_ column: Column, _ value: Column) -> Column {
  return fn("array_contains", column, value)
}

/// Returns null if the array is null, true if the array contains `value`, and false otherwise.
/// - Parameters:
///   - column: An array ``Column``.
///   - value: A literal value to check.
/// - Returns: A ``Column``.
public func array_contains(_ column: Column, _ value: some SparkLiteral) -> Column {
  return array_contains(column, value.toLiteralColumn)
}

/// Removes duplicate values from the array.
/// - Parameter col: An array ``Column``.
/// - Returns: A ``Column``.
public func array_distinct(_ col: Column) -> Column {
  return fn("array_distinct", col)
}

/// Returns an array of the elements in the first array but not in the second array, without
/// duplicates. The order of elements in the result is not determined.
/// - Parameters:
///   - col1: An array ``Column``.
///   - col2: An array ``Column``.
/// - Returns: A ``Column``.
public func array_except(_ col1: Column, _ col2: Column) -> Column {
  return fn("array_except", col1, col2)
}

/// Adds an item into a given array at a specified position.
/// - Parameters:
///   - arr: An array ``Column``.
///   - pos: A position ``Column``.
///   - value: A value ``Column`` to be inserted.
/// - Returns: A ``Column``.
public func array_insert(_ arr: Column, _ pos: Column, _ value: Column) -> Column {
  return fn("array_insert", arr, pos, value)
}

/// Returns an array of the elements in the intersection of the given two arrays, without
/// duplicates.
/// - Parameters:
///   - col1: An array ``Column``.
///   - col2: An array ``Column``.
/// - Returns: A ``Column``.
public func array_intersect(_ col1: Column, _ col2: Column) -> Column {
  return fn("array_intersect", col1, col2)
}

/// Concatenates the elements of the array column using the delimiter.
/// - Parameters:
///   - column: An array ``Column``.
///   - delimiter: A delimiter string.
/// - Returns: A ``Column``.
public func array_join(_ column: Column, _ delimiter: String) -> Column {
  return fn("array_join", column, lit(delimiter))
}

/// Concatenates the elements of the array column using the delimiter.
/// Null values are replaced with `nullReplacement`.
/// - Parameters:
///   - column: An array ``Column``.
///   - delimiter: A delimiter string.
///   - nullReplacement: A string to replace null values.
/// - Returns: A ``Column``.
public func array_join(_ column: Column, _ delimiter: String, _ nullReplacement: String) -> Column
{
  return fn("array_join", column, lit(delimiter), lit(nullReplacement))
}

/// Returns the maximum value in the array. NaN is greater than any non-NaN elements for
/// double/float type. Null elements will be ignored.
/// - Parameter col: An array ``Column``.
/// - Returns: A ``Column``.
public func array_max(_ col: Column) -> Column {
  return fn("array_max", col)
}

/// Returns the minimum value in the array. NaN is greater than any non-NaN elements for
/// double/float type. Null elements will be ignored.
/// - Parameter col: An array ``Column``.
/// - Returns: A ``Column``.
public func array_min(_ col: Column) -> Column {
  return fn("array_min", col)
}

/// Locates the position of the first occurrence of the value in the given array as long.
/// Returns null if either of the arguments are null.
/// The position is not zero based, but 1 based index. Returns 0 if the value could not be
/// found in the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - value: A value ``Column`` to locate.
/// - Returns: A ``Column``.
public func array_position(_ column: Column, _ value: Column) -> Column {
  return fn("array_position", column, value)
}

/// Locates the position of the first occurrence of the value in the given array as long.
/// Returns null if either of the arguments are null.
/// The position is not zero based, but 1 based index. Returns 0 if the value could not be
/// found in the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - value: A literal value to locate.
/// - Returns: A ``Column``.
public func array_position(_ column: Column, _ value: some SparkLiteral) -> Column {
  return array_position(column, value.toLiteralColumn)
}

/// Prepends the element to the beginning of the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - element: An element ``Column`` to be prepended.
/// - Returns: A ``Column``.
public func array_prepend(_ column: Column, _ element: Column) -> Column {
  return fn("array_prepend", column, element)
}

/// Prepends the element to the beginning of the array.
/// - Parameters:
///   - column: An array ``Column``.
///   - element: A literal element to be prepended.
/// - Returns: A ``Column``.
public func array_prepend(_ column: Column, _ element: some SparkLiteral) -> Column {
  return array_prepend(column, element.toLiteralColumn)
}

/// Removes all elements that equal to the element from the given array.
/// - Parameters:
///   - column: An array ``Column``.
///   - element: An element ``Column`` to be removed.
/// - Returns: A ``Column``.
public func array_remove(_ column: Column, _ element: Column) -> Column {
  return fn("array_remove", column, element)
}

/// Removes all elements that equal to the element from the given array.
/// - Parameters:
///   - column: An array ``Column``.
///   - element: A literal element to be removed.
/// - Returns: A ``Column``.
public func array_remove(_ column: Column, _ element: some SparkLiteral) -> Column {
  return array_remove(column, element.toLiteralColumn)
}

/// Creates an array containing the left argument repeated the number of times given by the
/// right argument.
/// - Parameters:
///   - left: A value ``Column`` to be repeated.
///   - right: A ``Column`` for the number of times.
/// - Returns: A ``Column``.
public func array_repeat(_ left: Column, _ right: Column) -> Column {
  return fn("array_repeat", left, right)
}

/// Creates an array containing the left argument repeated the number of times given by the
/// right argument.
/// - Parameters:
///   - col: A value ``Column`` to be repeated.
///   - count: The number of times.
/// - Returns: A ``Column``.
public func array_repeat(_ col: Column, _ count: Int32) -> Column {
  return array_repeat(col, lit(count))
}

/// Returns the total number of elements in the array. The function returns null for null input.
/// - Parameter col: An array ``Column``.
/// - Returns: A ``Column``.
public func array_size(_ col: Column) -> Column {
  return fn("array_size", col)
}

/// Sorts the input array in ascending order. The elements of the input array must be orderable.
/// NaN is greater than any non-NaN elements for double/float type. Null elements will be placed
/// at the end of the returned array.
/// - Parameter col: An array ``Column``.
/// - Returns: A ``Column``.
public func array_sort(_ col: Column) -> Column {
  return fn("array_sort", col)
}

/// Returns an array of the elements in the union of the given two arrays, without duplicates.
/// - Parameters:
///   - col1: An array ``Column``.
///   - col2: An array ``Column``.
/// - Returns: A ``Column``.
public func array_union(_ col1: Column, _ col2: Column) -> Column {
  return fn("array_union", col1, col2)
}

/// Returns true if `a1` and `a2` have at least one non-null element in common. If not and both
/// the arrays are non-empty and any of them contains a null, it returns null. It returns false
/// otherwise.
/// - Parameters:
///   - a1: An array ``Column``.
///   - a2: An array ``Column``.
/// - Returns: A ``Column``.
public func arrays_overlap(_ a1: Column, _ a2: Column) -> Column {
  return fn("arrays_overlap", a1, a2)
}

/// Returns a merged array of structs in which the N-th struct contains all N-th values of input
/// arrays.
/// - Parameter cols: Array ``Column``s to be merged.
/// - Returns: A ``Column``.
public func arrays_zip(_ cols: Column...) -> Column {
  return fn("arrays_zip", cols)
}

/// Returns the length of the array or map. This is an alias of ``size(_:)``.
/// - Parameter col: An array or map ``Column``.
/// - Returns: A ``Column``.
public func cardinality(_ col: Column) -> Column {
  return fn("cardinality", col)
}

/// Concatenates multiple input columns together into a single column. The function works with
/// strings, numeric, binary and compatible array columns.
/// - Parameter exprs: ``Column``s to be concatenated.
/// - Returns: A ``Column``.
public func concat(_ exprs: Column...) -> Column {
  return fn("concat", exprs)
}

/// Returns element of array at given index in `value` if `column` is array. Returns value for
/// the given key in `value` if `column` is map.
/// - Parameters:
///   - column: An array or map ``Column``.
///   - value: An index or key ``Column``.
/// - Returns: A ``Column``.
public func element_at(_ column: Column, _ value: Column) -> Column {
  return fn("element_at", column, value)
}

/// Returns element of array at given index in `value` if `column` is array. Returns value for
/// the given key in `value` if `column` is map.
/// - Parameters:
///   - column: An array or map ``Column``.
///   - value: A literal index or key.
/// - Returns: A ``Column``.
public func element_at(_ column: Column, _ value: some SparkLiteral) -> Column {
  return element_at(column, value.toLiteralColumn)
}

/// Creates a new row for each element in the given array or map column. Uses the default column
/// name `col` for elements in the array and `key` and `value` for elements in the map unless
/// specified otherwise.
/// - Parameter col: An array or map ``Column``.
/// - Returns: A ``Column``.
public func explode(_ col: Column) -> Column {
  return fn("explode", col)
}

/// Creates a new row for each element in the given array or map column. Uses the default column
/// name `col` for elements in the array and `key` and `value` for elements in the map unless
/// specified otherwise. Unlike ``explode(_:)``, if the array/map is null or empty then null is
/// produced.
/// - Parameter col: An array or map ``Column``.
/// - Returns: A ``Column``.
public func explode_outer(_ col: Column) -> Column {
  return fn("explode_outer", col)
}

/// Creates a single array from an array of arrays. If a structure of nested arrays is deeper
/// than two levels, only one level of nesting is removed.
/// - Parameter col: An array of arrays ``Column``.
/// - Returns: A ``Column``.
public func flatten(_ col: Column) -> Column {
  return fn("flatten", col)
}

/// Returns element of array at given (0-based) index. If the index points outside of the array
/// boundaries, then this function returns NULL.
/// - Parameters:
///   - column: An array ``Column``.
///   - index: An index ``Column``.
/// - Returns: A ``Column``.
public func get(_ column: Column, _ index: Column) -> Column {
  return fn("get", column, index)
}

/// Creates a new row for each element in the given array of structs.
/// - Parameter col: An array of structs ``Column``.
/// - Returns: A ``Column``.
public func inline(_ col: Column) -> Column {
  return fn("inline", col)
}

/// Creates a new row for each element in the given array of structs. Unlike ``inline(_:)``,
/// if the array is null or empty then null is produced for each nested column.
/// - Parameter col: An array of structs ``Column``.
/// - Returns: A ``Column``.
public func inline_outer(_ col: Column) -> Column {
  return fn("inline_outer", col)
}

/// Creates a new map column. The input columns must be grouped as key-value pairs, e.g.
/// (key1, value1, key2, value2, ...). The key columns must all have the same data type, and
/// can't be null. The value columns must all have the same data type.
/// - Parameter cols: ``Column``s grouped as key-value pairs.
/// - Returns: A ``Column``.
public func map(_ cols: Column...) -> Column {
  return fn("map", cols)
}

/// Returns the union of all the given maps.
/// - Parameter cols: Map ``Column``s to be unioned.
/// - Returns: A ``Column``.
public func map_concat(_ cols: Column...) -> Column {
  return fn("map_concat", cols)
}

/// Returns true if the map contains the key.
/// - Parameters:
///   - column: A map ``Column``.
///   - key: A key ``Column`` to check.
/// - Returns: A ``Column``.
public func map_contains_key(_ column: Column, _ key: Column) -> Column {
  return fn("map_contains_key", column, key)
}

/// Returns true if the map contains the key.
/// - Parameters:
///   - column: A map ``Column``.
///   - key: A literal key to check.
/// - Returns: A ``Column``.
public func map_contains_key(_ column: Column, _ key: some SparkLiteral) -> Column {
  return map_contains_key(column, key.toLiteralColumn)
}

/// Returns an unordered array of all entries in the given map.
/// - Parameter col: A map ``Column``.
/// - Returns: A ``Column``.
public func map_entries(_ col: Column) -> Column {
  return fn("map_entries", col)
}

/// Creates a new map column. The array in the first column is used for keys. The array in the
/// second column is used for values. All elements in the array for key should not be null.
/// - Parameters:
///   - keys: An array ``Column`` for keys.
///   - values: An array ``Column`` for values.
/// - Returns: A ``Column``.
public func map_from_arrays(_ keys: Column, _ values: Column) -> Column {
  return fn("map_from_arrays", keys, values)
}

/// Returns a map created from the given array of entries.
/// - Parameter col: An array of structs ``Column``.
/// - Returns: A ``Column``.
public func map_from_entries(_ col: Column) -> Column {
  return fn("map_from_entries", col)
}

/// Returns an unordered array containing the keys of the map.
/// - Parameter col: A map ``Column``.
/// - Returns: A ``Column``.
public func map_keys(_ col: Column) -> Column {
  return fn("map_keys", col)
}

/// Returns an unordered array containing the values of the map.
/// - Parameter col: A map ``Column``.
/// - Returns: A ``Column``.
public func map_values(_ col: Column) -> Column {
  return fn("map_values", col)
}

/// Creates a struct with the given field names and values.
/// - Parameter cols: ``Column``s grouped as name-value pairs, e.g.
///   (name1, val1, name2, val2, ...).
/// - Returns: A ``Column``.
public func named_struct(_ cols: Column...) -> Column {
  return fn("named_struct", cols)
}

/// Creates a new row for each element with position in the given array or map column. Uses the
/// default column name `pos` for position, and `col` for elements in the array and `key` and
/// `value` for elements in the map unless specified otherwise.
/// - Parameter col: An array or map ``Column``.
/// - Returns: A ``Column``.
public func posexplode(_ col: Column) -> Column {
  return fn("posexplode", col)
}

/// Creates a new row for each element with position in the given array or map column. Uses the
/// default column name `pos` for position, and `col` for elements in the array and `key` and
/// `value` for elements in the map unless specified otherwise. Unlike ``posexplode(_:)``, if
/// the array/map is null or empty then the row (null, null) is produced.
/// - Parameter col: An array or map ``Column``.
/// - Returns: A ``Column``.
public func posexplode_outer(_ col: Column) -> Column {
  return fn("posexplode_outer", col)
}

/// Returns a reversed string or an array with reverse order of elements.
/// - Parameter col: A string or array ``Column``.
/// - Returns: A ``Column``.
public func reverse(_ col: Column) -> Column {
  return fn("reverse", col)
}

/// Generates a sequence of integers from `start` to `stop`, incrementing by 1 if `start` is
/// less than or equal to `stop`, otherwise -1.
/// - Parameters:
///   - start: A start ``Column``.
///   - stop: A stop ``Column``.
/// - Returns: A ``Column``.
public func sequence(_ start: Column, _ stop: Column) -> Column {
  return fn("sequence", start, stop)
}

/// Generates a sequence of integers from `start` to `stop`, incrementing by `step`.
/// - Parameters:
///   - start: A start ``Column``.
///   - stop: A stop ``Column``.
///   - step: A step ``Column``.
/// - Returns: A ``Column``.
public func sequence(_ start: Column, _ stop: Column, _ step: Column) -> Column {
  return fn("sequence", start, stop, step)
}

/// Returns a random permutation of the given array.
/// - Parameter col: An array ``Column``.
/// - Returns: A ``Column``.
public func shuffle(_ col: Column) -> Column {
  return fn("shuffle", col)
}

/// Returns a random permutation of the given array with the given seed.
/// - Parameters:
///   - col: An array ``Column``.
///   - seed: A seed ``Column``.
/// - Returns: A ``Column``.
public func shuffle(_ col: Column, _ seed: Column) -> Column {
  return fn("shuffle", col, seed)
}

/// Returns the length of the array or map.
/// - Parameter col: An array or map ``Column``.
/// - Returns: A ``Column``.
public func size(_ col: Column) -> Column {
  return fn("size", col)
}

/// Returns an array containing all the elements in `x` from index `start` (or starting from the
/// end if `start` is negative) with the specified `length`.
/// - Parameters:
///   - x: An array ``Column``.
///   - start: A start index ``Column`` (1-based).
///   - length: A length ``Column``.
/// - Returns: A ``Column``.
public func slice(_ x: Column, _ start: Column, _ length: Column) -> Column {
  return fn("slice", x, start, length)
}

/// Returns an array containing all the elements in `x` from index `start` (or starting from the
/// end if `start` is negative) with the specified `length`.
/// - Parameters:
///   - x: An array ``Column``.
///   - start: A start index (1-based).
///   - length: A length.
/// - Returns: A ``Column``.
public func slice(_ x: Column, _ start: Int32, _ length: Int32) -> Column {
  return slice(x, lit(start), lit(length))
}

/// Sorts the input array in ascending order according to the natural ordering of the array
/// elements. Null elements will be placed at the beginning of the returned array.
/// - Parameter col: An array ``Column``.
/// - Returns: A ``Column``.
public func sort_array(_ col: Column) -> Column {
  return sort_array(col, true)
}

/// Sorts the input array in ascending or descending order according to the natural ordering of
/// the array elements. Null elements will be placed at the beginning of the returned array in
/// ascending order or at the end of the returned array in descending order.
/// - Parameters:
///   - col: An array ``Column``.
///   - asc: True for the ascending order.
/// - Returns: A ``Column``.
public func sort_array(_ col: Column, _ asc: Bool) -> Column {
  return fn("sort_array", col, lit(asc))
}

/// Separates the columns into the specified number of rows.
/// - Parameter cols: ``Column``s where the first is the number of rows, followed by values.
/// - Returns: A ``Column``.
public func stack(_ cols: Column...) -> Column {
  return fn("stack", cols)
}

/// Creates a map after splitting the text into key/value pairs using delimiters. The default
/// delimiters are `,` for `pairDelim` and `:` for `keyValueDelim`.
/// - Parameter text: A text ``Column``.
/// - Returns: A ``Column``.
public func str_to_map(_ text: Column) -> Column {
  return fn("str_to_map", text)
}

/// Creates a map after splitting the text into key/value pairs using delimiters. The default
/// delimiter is `:` for `keyValueDelim`.
/// - Parameters:
///   - text: A text ``Column``.
///   - pairDelim: A pair delimiter ``Column``.
/// - Returns: A ``Column``.
public func str_to_map(_ text: Column, _ pairDelim: Column) -> Column {
  return fn("str_to_map", text, pairDelim)
}

/// Creates a map after splitting the text into key/value pairs using delimiters.
/// - Parameters:
///   - text: A text ``Column``.
///   - pairDelim: A pair delimiter ``Column``.
///   - keyValueDelim: A key/value delimiter ``Column``.
/// - Returns: A ``Column``.
public func str_to_map(_ text: Column, _ pairDelim: Column, _ keyValueDelim: Column) -> Column {
  return fn("str_to_map", text, pairDelim, keyValueDelim)
}

/// Creates a new struct column.
/// - Parameter cols: ``Column``s to be combined into a struct.
/// - Returns: A ``Column``.
public func `struct`(_ cols: Column...) -> Column {
  return fn("struct", cols)
}

/// Returns element of array at given (1-based) index. If the index points outside of the array
/// boundaries, then this function returns NULL. Returns value for the given key in map, or NULL
/// if the key is not contained in the map.
/// - Parameters:
///   - column: An array or map ``Column``.
///   - value: An index or key ``Column``.
/// - Returns: A ``Column``.
public func try_element_at(_ column: Column, _ value: Column) -> Column {
  return fn("try_element_at", column, value)
}
