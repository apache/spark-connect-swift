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

// MARK: - Math functions

/// Computes the absolute value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func abs(_ col: Column) -> Column {
  return fn("abs", col)
}

/// Computes the inverse cosine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func acos(_ col: Column) -> Column {
  return fn("acos", col)
}

/// Computes the inverse hyperbolic cosine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func acosh(_ col: Column) -> Column {
  return fn("acosh", col)
}

/// Computes the inverse sine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func asin(_ col: Column) -> Column {
  return fn("asin", col)
}

/// Computes the inverse hyperbolic sine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func asinh(_ col: Column) -> Column {
  return fn("asinh", col)
}

/// Computes the inverse tangent of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func atan(_ col: Column) -> Column {
  return fn("atan", col)
}

/// Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar
/// coordinates (r, theta).
/// - Parameters:
///   - y: A coordinate ``Column`` on the y-axis.
///   - x: A coordinate ``Column`` on the x-axis.
/// - Returns: A ``Column``.
public func atan2(_ y: Column, _ x: Column) -> Column {
  return fn("atan2", y, x)
}

/// Computes the inverse hyperbolic tangent of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func atanh(_ col: Column) -> Column {
  return fn("atanh", col)
}

/// Returns the string representation of the binary value of the given long column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func bin(_ col: Column) -> Column {
  return fn("bin", col)
}

/// Returns the value of the column rounded to 0 decimal places with HALF_EVEN round mode.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func bround(_ col: Column) -> Column {
  return bround(col, 0)
}

/// Rounds the value of the column to `scale` decimal places with HALF_EVEN round mode.
/// - Parameters:
///   - col: A ``Column``.
///   - scale: The number of decimal places.
/// - Returns: A ``Column``.
public func bround(_ col: Column, _ scale: Int32) -> Column {
  return fn("bround", col, lit(scale))
}

/// Computes the cube-root of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func cbrt(_ col: Column) -> Column {
  return fn("cbrt", col)
}

/// Computes the ceiling of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func ceil(_ col: Column) -> Column {
  return fn("ceil", col)
}

/// Computes the ceiling of the given value of `col` to `scale` decimal places.
/// - Parameters:
///   - col: A ``Column``.
///   - scale: A ``Column`` for the number of decimal places.
/// - Returns: A ``Column``.
public func ceil(_ col: Column, _ scale: Column) -> Column {
  return fn("ceil", col, scale)
}

/// Computes the ceiling of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func ceiling(_ col: Column) -> Column {
  return fn("ceiling", col)
}

/// Computes the ceiling of the given value of `col` to `scale` decimal places.
/// - Parameters:
///   - col: A ``Column``.
///   - scale: A ``Column`` for the number of decimal places.
/// - Returns: A ``Column``.
public func ceiling(_ col: Column, _ scale: Column) -> Column {
  return fn("ceiling", col, scale)
}

/// Converts a number in a string column from one base to another.
/// - Parameters:
///   - num: A ``Column`` to convert.
///   - fromBase: A base of the given number.
///   - toBase: A base to convert the number to.
/// - Returns: A ``Column``.
public func conv(_ num: Column, _ fromBase: Int32, _ toBase: Int32) -> Column {
  return fn("conv", num, lit(fromBase), lit(toBase))
}

/// Computes the cosine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func cos(_ col: Column) -> Column {
  return fn("cos", col)
}

/// Computes the hyperbolic cosine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func cosh(_ col: Column) -> Column {
  return fn("cosh", col)
}

/// Computes the cotangent of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func cot(_ col: Column) -> Column {
  return fn("cot", col)
}

/// Computes the cosecant of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func csc(_ col: Column) -> Column {
  return fn("csc", col)
}

/// Converts an angle measured in radians to an approximately equivalent angle measured in
/// degrees.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func degrees(_ col: Column) -> Column {
  return fn("degrees", col)
}

/// Returns Euler's number.
/// - Returns: A ``Column``.
public func e() -> Column {
  return fn("e")
}

/// Computes the exponential of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func exp(_ col: Column) -> Column {
  return fn("exp", col)
}

/// Computes the exponential of the given value minus one.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func expm1(_ col: Column) -> Column {
  return fn("expm1", col)
}

/// Computes the factorial of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func factorial(_ col: Column) -> Column {
  return fn("factorial", col)
}

/// Computes the floor of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func floor(_ col: Column) -> Column {
  return fn("floor", col)
}

/// Computes the floor of the given value of `col` to `scale` decimal places.
/// - Parameters:
///   - col: A ``Column``.
///   - scale: A ``Column`` for the number of decimal places.
/// - Returns: A ``Column``.
public func floor(_ col: Column, _ scale: Column) -> Column {
  return fn("floor", col, scale)
}

/// Returns the greatest value of the list of values, skipping null values. This function takes
/// at least 2 parameters. It will return null iff all parameters are null.
/// - Parameters:
///   - col1: A ``Column``.
///   - col2: A ``Column``.
///   - cols: Additional ``Column``s.
/// - Returns: A ``Column``.
public func greatest(_ col1: Column, _ col2: Column, _ cols: Column...) -> Column {
  return fn("greatest", [col1, col2] + cols)
}

/// Computes hex value of the given column.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func hex(_ col: Column) -> Column {
  return fn("hex", col)
}

/// Computes `sqrt(l^2 + r^2)` without intermediate overflow or underflow.
/// - Parameters:
///   - l: A ``Column``.
///   - r: A ``Column``.
/// - Returns: A ``Column``.
public func hypot(_ l: Column, _ r: Column) -> Column {
  return fn("hypot", l, r)
}

/// Returns the least value of the list of values, skipping null values. This function takes at
/// least 2 parameters. It will return null iff all parameters are null.
/// - Parameters:
///   - col1: A ``Column``.
///   - col2: A ``Column``.
///   - cols: Additional ``Column``s.
/// - Returns: A ``Column``.
public func least(_ col1: Column, _ col2: Column, _ cols: Column...) -> Column {
  return fn("least", [col1, col2] + cols)
}

/// Computes the natural logarithm of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func ln(_ col: Column) -> Column {
  return fn("ln", col)
}

/// Computes the natural logarithm of the given value. This is an alias of ``ln(_:)``.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func log(_ col: Column) -> Column {
  return ln(col)
}

/// Returns the first argument-base logarithm of the second argument.
/// - Parameters:
///   - base: A base of the logarithm.
///   - col: A ``Column``.
/// - Returns: A ``Column``.
public func log(_ base: Double, _ col: Column) -> Column {
  return fn("log", lit(base), col)
}

/// Computes the logarithm of the given value in base 10.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func log10(_ col: Column) -> Column {
  return fn("log10", col)
}

/// Computes the natural logarithm of the given value plus one.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func log1p(_ col: Column) -> Column {
  return fn("log1p", col)
}

/// Computes the logarithm of the given column in base 2.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func log2(_ col: Column) -> Column {
  return fn("log2", col)
}

/// Returns the negated value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func negative(_ col: Column) -> Column {
  return fn("negative", col)
}

/// Returns Pi.
/// - Returns: A ``Column``.
public func pi() -> Column {
  return fn("pi")
}

/// Returns the positive value of dividend mod divisor.
/// - Parameters:
///   - dividend: A dividend ``Column``.
///   - divisor: A divisor ``Column``.
/// - Returns: A ``Column``.
public func pmod(_ dividend: Column, _ divisor: Column) -> Column {
  return fn("pmod", dividend, divisor)
}

/// Returns the value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func positive(_ col: Column) -> Column {
  return fn("positive", col)
}

/// Returns the value of the first argument raised to the power of the second argument.
/// - Parameters:
///   - l: A base ``Column``.
///   - r: An exponent ``Column``.
/// - Returns: A ``Column``.
public func pow(_ l: Column, _ r: Column) -> Column {
  return fn("power", l, r)
}

/// Returns the value of the first argument raised to the power of the second argument.
/// This is an alias of ``pow(_:_:)``.
/// - Parameters:
///   - l: A base ``Column``.
///   - r: An exponent ``Column``.
/// - Returns: A ``Column``.
public func power(_ l: Column, _ r: Column) -> Column {
  return fn("power", l, r)
}

/// Converts an angle measured in degrees to an approximately equivalent angle measured in
/// radians.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func radians(_ col: Column) -> Column {
  return fn("radians", col)
}

/// Generates a random column with independent and identically distributed (i.i.d.) samples
/// uniformly distributed in [0.0, 1.0). The function is non-deterministic in general case.
/// - Returns: A ``Column``.
public func rand() -> Column {
  return fn("rand")
}

/// Generates a random column with independent and identically distributed (i.i.d.) samples
/// uniformly distributed in [0.0, 1.0), with the chosen random seed.
/// - Parameter seed: A random seed.
/// - Returns: A ``Column``.
public func rand(_ seed: Int64) -> Column {
  return fn("rand", lit(seed))
}

/// Generates a column with independent and identically distributed (i.i.d.) samples from the
/// standard normal distribution. The function is non-deterministic in general case.
/// - Returns: A ``Column``.
public func randn() -> Column {
  return fn("randn")
}

/// Generates a column with independent and identically distributed (i.i.d.) samples from the
/// standard normal distribution, with the chosen random seed.
/// - Parameter seed: A random seed.
/// - Returns: A ``Column``.
public func randn(_ seed: Int64) -> Column {
  return fn("randn", lit(seed))
}

/// Returns the double value that is closest in value to the argument and is equal to a
/// mathematical integer.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func rint(_ col: Column) -> Column {
  return fn("rint", col)
}

/// Returns the value of the column rounded to 0 decimal places with HALF_UP round mode.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func round(_ col: Column) -> Column {
  return round(col, 0)
}

/// Rounds the value of the column to `scale` decimal places with HALF_UP round mode.
/// - Parameters:
///   - col: A ``Column``.
///   - scale: The number of decimal places.
/// - Returns: A ``Column``.
public func round(_ col: Column, _ scale: Int32) -> Column {
  return fn("round", col, lit(scale))
}

/// Computes the secant of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func sec(_ col: Column) -> Column {
  return fn("sec", col)
}

/// Computes the signum of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func sign(_ col: Column) -> Column {
  return fn("sign", col)
}

/// Computes the signum of the given value. This is an alias of ``sign(_:)``.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func signum(_ col: Column) -> Column {
  return fn("signum", col)
}

/// Computes the sine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func sin(_ col: Column) -> Column {
  return fn("sin", col)
}

/// Computes the hyperbolic sine of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func sinh(_ col: Column) -> Column {
  return fn("sinh", col)
}

/// Computes the square root of the specified value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func sqrt(_ col: Column) -> Column {
  return fn("sqrt", col)
}

/// Computes the tangent of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func tan(_ col: Column) -> Column {
  return fn("tan", col)
}

/// Computes the hyperbolic tangent of the given value.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func tanh(_ col: Column) -> Column {
  return fn("tanh", col)
}

/// Returns the sum of `left` and `right` and the result is null on overflow. The acceptable
/// input types are the same with the `+` operator.
/// - Parameters:
///   - left: A left operand ``Column``.
///   - right: A right operand ``Column``.
/// - Returns: A ``Column``.
public func try_add(_ left: Column, _ right: Column) -> Column {
  return fn("try_add", left, right)
}

/// Returns `left`/`right`. It always performs floating point division. Its result is always null
/// if `right` is 0.
/// - Parameters:
///   - left: A dividend ``Column``.
///   - right: A divisor ``Column``.
/// - Returns: A ``Column``.
public func try_divide(_ left: Column, _ right: Column) -> Column {
  return fn("try_divide", left, right)
}

/// Returns the remainder of `left`/`right`. Its result is always null if `right` is 0.
/// - Parameters:
///   - left: A dividend ``Column``.
///   - right: A divisor ``Column``.
/// - Returns: A ``Column``.
public func try_mod(_ left: Column, _ right: Column) -> Column {
  return fn("try_mod", left, right)
}

/// Returns `left`*`right` and the result is null on overflow. The acceptable input types are the
/// same with the `*` operator.
/// - Parameters:
///   - left: A multiplicand ``Column``.
///   - right: A multiplier ``Column``.
/// - Returns: A ``Column``.
public func try_multiply(_ left: Column, _ right: Column) -> Column {
  return fn("try_multiply", left, right)
}

/// Returns `left`-`right` and the result is null on overflow. The acceptable input types are the
/// same with the `-` operator.
/// - Parameters:
///   - left: A left operand ``Column``.
///   - right: A right operand ``Column``.
/// - Returns: A ``Column``.
public func try_subtract(_ left: Column, _ right: Column) -> Column {
  return fn("try_subtract", left, right)
}

/// Inverse of ``hex(_:)``. Interprets each pair of characters as a hexadecimal number and
/// converts to the byte representation of number.
/// - Parameter col: A ``Column``.
/// - Returns: A ``Column``.
public func unhex(_ col: Column) -> Column {
  return fn("unhex", col)
}

/// Returns a random value with independent and identically distributed (i.i.d.) values with the
/// specified range of numbers. The provided numbers specifying the minimum and maximum values of
/// the range must be constant. If both of these numbers are integers, then the result will also
/// be an integer. Otherwise if one or both of these are floating-point numbers, then the result
/// will also be a floating-point number.
/// - Parameters:
///   - min: A minimum value ``Column`` in the range. Must be a constant.
///   - max: A maximum value ``Column`` in the range. Must be a constant.
/// - Returns: A ``Column``.
public func uniform(_ min: Column, _ max: Column) -> Column {
  return fn("uniform", min, max)
}

/// Returns a random value with independent and identically distributed (i.i.d.) values with the
/// specified range of numbers, with the chosen random seed. The provided numbers specifying the
/// minimum and maximum values of the range must be constant. If both of these numbers are
/// integers, then the result will also be an integer. Otherwise if one or both of these are
/// floating-point numbers, then the result will also be a floating-point number.
/// - Parameters:
///   - min: A minimum value ``Column`` in the range. Must be a constant.
///   - max: A maximum value ``Column`` in the range. Must be a constant.
///   - seed: A random seed ``Column``. Must be a constant.
/// - Returns: A ``Column``.
public func uniform(_ min: Column, _ max: Column, _ seed: Column) -> Column {
  return fn("uniform", min, max, seed)
}

/// Returns the bucket number into which the value of this expression would fall after being
/// evaluated.
/// - Parameters:
///   - v: A value ``Column`` for which the bucket is computed.
///   - min: A minimum value ``Column`` of the histogram.
///   - max: A maximum value ``Column`` of the histogram.
///   - numBucket: A ``Column`` for the number of buckets.
/// - Returns: A ``Column``.
public func width_bucket(_ v: Column, _ min: Column, _ max: Column, _ numBucket: Column) -> Column
{
  return fn("width_bucket", v, min, max, numBucket)
}
