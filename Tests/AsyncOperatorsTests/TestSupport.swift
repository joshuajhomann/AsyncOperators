import Synchronization

final class LockedBox<Value: Sendable>: Sendable {
  private let storage: Mutex<Value>

  init(_ value: Value) {
    storage = Mutex(value)
  }

  var value: Value {
    storage.withLock { $0 }
  }

  func set(_ newValue: Value) {
    storage.withLock { $0 = newValue }
  }
}

final class Recorder<Value: Sendable>: Sendable {
  private let storage = Mutex<[Value]>([])

  var values: [Value] {
    storage.withLock { $0 }
  }

  func append(_ value: Value) {
    storage.withLock { $0.append(value) }
  }
}

enum FixtureError: Error, Equatable, Sendable {
  case expected
}

struct FixtureSequence: AsyncSequence, Sendable {
  typealias Element = Int
  typealias Failure = FixtureError

  enum Termination: Sendable {
    case finished
    case failed
  }

  struct AsyncIterator: AsyncIteratorProtocol {
    var elements: ArraySlice<Int>
    let termination: Termination
    var reachedEnd = false

    mutating func next() async throws(FixtureError) -> Int? {
      if let element = elements.popFirst() {
        return element
      }

      guard !reachedEnd else { return nil }
      reachedEnd = true

      switch termination {
      case .finished:
        return nil
      case .failed:
        throw .expected
      }
    }
  }

  let elements: [Int]
  let termination: Termination

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(elements: elements[...], termination: termination)
  }
}
