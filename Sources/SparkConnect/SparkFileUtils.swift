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
import Foundation

/// Utility functions like `org.apache.spark.util.SparkFileUtils`.
public enum SparkFileUtils {

  /// Return a well-formed URL for the file described by a user input string.
  ///
  /// If the supplied path does not contain a scheme, or is a relative path, it will be
  /// converted into an absolute path with a file:// scheme.
  ///
  /// - Parameter path: A path string.
  /// - Returns: An URL
  static func resolveURL(_ path: String) -> URL? {
    if let url = URL(string: path) {
      if url.scheme != nil {
        return url.absoluteURL
      }

      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if let fragment = url.fragment {
        var components = URLComponents()
        components.scheme = "file"
        components.path = (path as NSString).expandingTildeInPath
        components.fragment = fragment
        return components.url?.absoluteURL
      }
    }
    return URL(fileURLWithPath: (path as NSString).expandingTildeInPath).absoluteURL
  }

  /// Lists files recursively.
  /// - Parameter directory: <#directory description#>
  /// - Returns: <#description#>
  static func recursiveList(directory: URL) -> [URL] {
    let fileManager = FileManager.default
    var results: [URL] = []
    if let enumerator = fileManager.enumerator(at: directory, includingPropertiesForKeys: nil) {
      for case let fileURL as URL in enumerator {
        results.append(fileURL)
      }
    }
    return results
  }

  /// Create a directory given the abstract pathname
  /// - Parameter url: An URL location.
  /// - Returns: Return true if the directory is successfully created; otherwise, return false.
  @discardableResult
  static func createDirectory(at url: URL) -> Bool {
    let fileManager = FileManager.default
    do {
      try fileManager.createDirectory(at: url, withIntermediateDirectories: true)
      var isDir: ObjCBool = false
      let exists = fileManager.fileExists(atPath: url.path, isDirectory: &isDir)
      return exists && isDir.boolValue
    } catch {
      print("Failed to create directory: \(url.path), error: \(error)")
      return false
    }
  }

  /// Create a temporary directory inside the given parent directory.
  /// - Parameters:
  ///   - root: A parent directory.
  ///   - namePrefix: A prefix for a new directory name.
  /// - Returns: An URL for the created directory
  static func createDirectory(root: String, namePrefix: String = "spark") -> URL {
    let tempDir = URL(fileURLWithPath: root).appendingPathComponent(
      "\(namePrefix)-\(UUID().uuidString)")
    createDirectory(at: tempDir)
    return tempDir
  }

  /// Create a new temporary directory prefixed with `spark` inside ``NSTemporaryDirectory``.
  /// - Returns: An URL for the created directory
  static func createTempDir() -> URL {
    let dir = createDirectory(root: NSTemporaryDirectory(), namePrefix: "spark")

    return dir
  }

  /// Delete a file or directory and its contents recursively.
  /// Throws an exception if deletion is unsuccessful.
  /// - Parameter url: An URL location.
  static func deleteRecursively(_ url: URL) throws {
    let fileManager = FileManager.default
    if fileManager.fileExists(atPath: url.path) {
      try fileManager.removeItem(at: url)
    } else {
      throw SparkConnectError.InvalidArgumentException
    }
  }
}
