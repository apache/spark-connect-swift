// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// automatically generated by the FlatBuffers compiler, do not modify
// swiftlint:disable all
// swiftformat:disable all

import FlatBuffers

///  ----------------------------------------------------------------------
///  Data structures for dense tensors
///  Shape data for a single axis in a tensor
public struct org_apache_arrow_flatbuf_TensorDim: FlatBufferObject, Verifiable {

  static func validateVersion() { FlatBuffersVersion_23_1_4() }
  public var __buffer: ByteBuffer! { return _accessor.bb }
  private var _accessor: Table

  public static func getRootAsTensorDim(bb: ByteBuffer) -> org_apache_arrow_flatbuf_TensorDim {
    return org_apache_arrow_flatbuf_TensorDim(
      Table(
        bb: bb, position: Int32(bb.read(def: UOffset.self, position: bb.reader)) + Int32(bb.reader))
    )
  }

  private init(_ t: Table) { _accessor = t }
  public init(_ bb: ByteBuffer, o: Int32) { _accessor = Table(bb: bb, position: o) }

  private enum VTOFFSET: VOffset {
    case size = 4
    case name = 6
    var v: Int32 { Int32(self.rawValue) }
    var p: VOffset { self.rawValue }
  }

  ///  Length of dimension
  public var size: Int64 {
    let o = _accessor.offset(VTOFFSET.size.v)
    return o == 0 ? 0 : _accessor.readBuffer(of: Int64.self, at: o)
  }
  ///  Name of the dimension, optional
  public var name: String? {
    let o = _accessor.offset(VTOFFSET.name.v)
    return o == 0 ? nil : _accessor.string(at: o)
  }
  public var nameSegmentArray: [UInt8]? { return _accessor.getVector(at: VTOFFSET.name.v) }
  public static func startTensorDim(_ fbb: inout FlatBufferBuilder) -> UOffset {
    fbb.startTable(with: 2)
  }
  public static func add(size: Int64, _ fbb: inout FlatBufferBuilder) {
    fbb.add(element: size, def: 0, at: VTOFFSET.size.p)
  }
  public static func add(name: Offset, _ fbb: inout FlatBufferBuilder) {
    fbb.add(offset: name, at: VTOFFSET.name.p)
  }
  public static func endTensorDim(_ fbb: inout FlatBufferBuilder, start: UOffset) -> Offset {
    let end = Offset(offset: fbb.endTable(at: start))
    return end
  }
  public static func createTensorDim(
    _ fbb: inout FlatBufferBuilder,
    size: Int64 = 0,
    nameOffset name: Offset = Offset()
  ) -> Offset {
    let __start = org_apache_arrow_flatbuf_TensorDim.startTensorDim(&fbb)
    org_apache_arrow_flatbuf_TensorDim.add(size: size, &fbb)
    org_apache_arrow_flatbuf_TensorDim.add(name: name, &fbb)
    return org_apache_arrow_flatbuf_TensorDim.endTensorDim(&fbb, start: __start)
  }

  public static func verify<T>(_ verifier: inout Verifier, at position: Int, of type: T.Type) throws
  where T: Verifiable {
    var _v = try verifier.visitTable(at: position)
    try _v.visit(field: VTOFFSET.size.p, fieldName: "size", required: false, type: Int64.self)
    try _v.visit(
      field: VTOFFSET.name.p, fieldName: "name", required: false, type: ForwardOffset<String>.self)
    _v.finish()
  }
}

public struct org_apache_arrow_flatbuf_Tensor: FlatBufferObject, Verifiable {

  static func validateVersion() { FlatBuffersVersion_23_1_4() }
  public var __buffer: ByteBuffer! { return _accessor.bb }
  private var _accessor: Table

  public static func getRootAsTensor(bb: ByteBuffer) -> org_apache_arrow_flatbuf_Tensor {
    return org_apache_arrow_flatbuf_Tensor(
      Table(
        bb: bb, position: Int32(bb.read(def: UOffset.self, position: bb.reader)) + Int32(bb.reader))
    )
  }

  private init(_ t: Table) { _accessor = t }
  public init(_ bb: ByteBuffer, o: Int32) { _accessor = Table(bb: bb, position: o) }

  private enum VTOFFSET: VOffset {
    case typeType = 4
    case type = 6
    case shape = 8
    case strides = 10
    case data = 12
    var v: Int32 { Int32(self.rawValue) }
    var p: VOffset { self.rawValue }
  }

  public var typeType: org_apache_arrow_flatbuf_Type_ {
    let o = _accessor.offset(VTOFFSET.typeType.v)
    return o == 0
      ? .none_
      : org_apache_arrow_flatbuf_Type_(rawValue: _accessor.readBuffer(of: UInt8.self, at: o))
        ?? .none_
  }
  ///  The type of data contained in a value cell. Currently only fixed-width
  ///  value types are supported, no strings or nested types
  public func type<T: FlatbuffersInitializable>(type: T.Type) -> T! {
    let o = _accessor.offset(VTOFFSET.type.v)
    return _accessor.union(o)
  }
  ///  The dimensions of the tensor, optionally named
  public var hasShape: Bool {
    let o = _accessor.offset(VTOFFSET.shape.v)
    return o == 0 ? false : true
  }
  public var shapeCount: Int32 {
    let o = _accessor.offset(VTOFFSET.shape.v)
    return o == 0 ? 0 : _accessor.vector(count: o)
  }
  public func shape(at index: Int32) -> org_apache_arrow_flatbuf_TensorDim? {
    let o = _accessor.offset(VTOFFSET.shape.v)
    return o == 0
      ? nil
      : org_apache_arrow_flatbuf_TensorDim(
        _accessor.bb, o: _accessor.indirect(_accessor.vector(at: o) + index * 4))
  }
  ///  Non-negative byte offsets to advance one value cell along each dimension
  ///  If omitted, default to row-major order (C-like).
  public var hasStrides: Bool {
    let o = _accessor.offset(VTOFFSET.strides.v)
    return o == 0 ? false : true
  }
  public var stridesCount: Int32 {
    let o = _accessor.offset(VTOFFSET.strides.v)
    return o == 0 ? 0 : _accessor.vector(count: o)
  }
  public func strides(at index: Int32) -> Int64 {
    let o = _accessor.offset(VTOFFSET.strides.v)
    return o == 0
      ? 0 : _accessor.directRead(of: Int64.self, offset: _accessor.vector(at: o) + index * 8)
  }
  public var strides: [Int64] { return _accessor.getVector(at: VTOFFSET.strides.v) ?? [] }
  ///  The location and size of the tensor's data
  public var data: org_apache_arrow_flatbuf_Buffer! {
    let o = _accessor.offset(VTOFFSET.data.v)
    return _accessor.readBuffer(of: org_apache_arrow_flatbuf_Buffer.self, at: o)
  }
  public var mutableData: org_apache_arrow_flatbuf_Buffer_Mutable! {
    let o = _accessor.offset(VTOFFSET.data.v)
    return org_apache_arrow_flatbuf_Buffer_Mutable(_accessor.bb, o: o + _accessor.postion)
  }
  public static func startTensor(_ fbb: inout FlatBufferBuilder) -> UOffset {
    fbb.startTable(with: 5)
  }
  public static func add(typeType: org_apache_arrow_flatbuf_Type_, _ fbb: inout FlatBufferBuilder) {
    fbb.add(element: typeType.rawValue, def: 0, at: VTOFFSET.typeType.p)
  }
  public static func add(type: Offset, _ fbb: inout FlatBufferBuilder) {
    fbb.add(offset: type, at: VTOFFSET.type.p)
  }
  public static func addVectorOf(shape: Offset, _ fbb: inout FlatBufferBuilder) {
    fbb.add(offset: shape, at: VTOFFSET.shape.p)
  }
  public static func addVectorOf(strides: Offset, _ fbb: inout FlatBufferBuilder) {
    fbb.add(offset: strides, at: VTOFFSET.strides.p)
  }
  public static func add(data: org_apache_arrow_flatbuf_Buffer?, _ fbb: inout FlatBufferBuilder) {
    guard let data = data else { return }
    fbb.create(struct: data, position: VTOFFSET.data.p)
  }
  public static func endTensor(_ fbb: inout FlatBufferBuilder, start: UOffset) -> Offset {
    let end = Offset(offset: fbb.endTable(at: start))
    fbb.require(table: end, fields: [6, 8, 12])
    return end
  }
  public static func createTensor(
    _ fbb: inout FlatBufferBuilder,
    typeType: org_apache_arrow_flatbuf_Type_ = .none_,
    typeOffset type: Offset,
    shapeVectorOffset shape: Offset,
    stridesVectorOffset strides: Offset = Offset(),
    data: org_apache_arrow_flatbuf_Buffer
  ) -> Offset {
    let __start = org_apache_arrow_flatbuf_Tensor.startTensor(&fbb)
    org_apache_arrow_flatbuf_Tensor.add(typeType: typeType, &fbb)
    org_apache_arrow_flatbuf_Tensor.add(type: type, &fbb)
    org_apache_arrow_flatbuf_Tensor.addVectorOf(shape: shape, &fbb)
    org_apache_arrow_flatbuf_Tensor.addVectorOf(strides: strides, &fbb)
    org_apache_arrow_flatbuf_Tensor.add(data: data, &fbb)
    return org_apache_arrow_flatbuf_Tensor.endTensor(&fbb, start: __start)
  }

  public static func verify<T>(_ verifier: inout Verifier, at position: Int, of type: T.Type) throws
  where T: Verifiable {
    var _v = try verifier.visitTable(at: position)
    try _v.visit(
      unionKey: VTOFFSET.typeType.p, unionField: VTOFFSET.type.p, unionKeyName: "typeType",
      fieldName: "type", required: true,
      completion: { (verifier, key: org_apache_arrow_flatbuf_Type_, pos) in
        switch key {
        case .none_:
          break  // NOTE - SWIFT doesnt support none
        case .null:
          try ForwardOffset<org_apache_arrow_flatbuf_Null>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Null.self)
        case .int:
          try ForwardOffset<org_apache_arrow_flatbuf_Int>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Int.self)
        case .floatingpoint:
          try ForwardOffset<org_apache_arrow_flatbuf_FloatingPoint>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_FloatingPoint.self)
        case .binary:
          try ForwardOffset<org_apache_arrow_flatbuf_Binary>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Binary.self)
        case .utf8:
          try ForwardOffset<org_apache_arrow_flatbuf_Utf8>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Utf8.self)
        case .bool:
          try ForwardOffset<org_apache_arrow_flatbuf_Bool>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Bool.self)
        case .decimal:
          try ForwardOffset<org_apache_arrow_flatbuf_Decimal>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Decimal.self)
        case .date:
          try ForwardOffset<org_apache_arrow_flatbuf_Date>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Date.self)
        case .time:
          try ForwardOffset<org_apache_arrow_flatbuf_Time>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Time.self)
        case .timestamp:
          try ForwardOffset<org_apache_arrow_flatbuf_Timestamp>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Timestamp.self)
        case .interval:
          try ForwardOffset<org_apache_arrow_flatbuf_Interval>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Interval.self)
        case .list:
          try ForwardOffset<org_apache_arrow_flatbuf_List>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_List.self)
        case .struct_:
          try ForwardOffset<org_apache_arrow_flatbuf_Struct_>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Struct_.self)
        case .union:
          try ForwardOffset<org_apache_arrow_flatbuf_Union>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Union.self)
        case .fixedsizebinary:
          try ForwardOffset<org_apache_arrow_flatbuf_FixedSizeBinary>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_FixedSizeBinary.self)
        case .fixedsizelist:
          try ForwardOffset<org_apache_arrow_flatbuf_FixedSizeList>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_FixedSizeList.self)
        case .map:
          try ForwardOffset<org_apache_arrow_flatbuf_Map>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Map.self)
        case .duration:
          try ForwardOffset<org_apache_arrow_flatbuf_Duration>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_Duration.self)
        case .largebinary:
          try ForwardOffset<org_apache_arrow_flatbuf_LargeBinary>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_LargeBinary.self)
        case .largeutf8:
          try ForwardOffset<org_apache_arrow_flatbuf_LargeUtf8>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_LargeUtf8.self)
        case .largelist:
          try ForwardOffset<org_apache_arrow_flatbuf_LargeList>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_LargeList.self)
        case .runendencoded:
          try ForwardOffset<org_apache_arrow_flatbuf_RunEndEncoded>.verify(
            &verifier, at: pos, of: org_apache_arrow_flatbuf_RunEndEncoded.self)
        }
      })
    try _v.visit(
      field: VTOFFSET.shape.p, fieldName: "shape", required: true,
      type: ForwardOffset<
        Vector<
          ForwardOffset<org_apache_arrow_flatbuf_TensorDim>, org_apache_arrow_flatbuf_TensorDim
        >
      >.self)
    try _v.visit(
      field: VTOFFSET.strides.p, fieldName: "strides", required: false,
      type: ForwardOffset<Vector<Int64, Int64>>.self)
    try _v.visit(
      field: VTOFFSET.data.p, fieldName: "data", required: true,
      type: org_apache_arrow_flatbuf_Buffer.self)
    _v.finish()
  }
}
