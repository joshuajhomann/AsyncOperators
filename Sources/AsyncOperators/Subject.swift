import Foundation
import Synchronization

/// A multicast source that starts each consumer with the current value.
nonisolated public final class Subject<Element: Sendable>: Sendable {
  typealias AsyncIterator = AsyncStream<Element>.AsyncIterator
  typealias Continuation = AsyncStream<Element>.Continuation

  private struct State: Sendable {
    var value: Element
    var continuations: [UUID: Continuation] = [:]
    var isFinished = false
  }

  private let state: Mutex<State>

  public var value: Element {
    get {
      state.withLock { $0.value }
    }
    set {
      send(newValue)
    }
  }

  public init(_ value: Element) {
    state = Mutex(State(value: value))
  }

  deinit {
    finish()
  }

  /// Stores and sends a new current value to every consumer.
  public func send(_ newValue: Element) {
    state.withLock { state in
      guard !state.isFinished else { return }
      state.value = newValue
      for continuation in state.continuations.values {
        continuation.yield(newValue)
      }
    }
  }

  /// Permanently finishes the subject and all current consumers.
  public func finish() {
    let continuations = state.withLock { state in
      guard !state.isFinished else { return [Continuation]() }
      state.isFinished = true
      let result = Array(state.continuations.values)
      state.continuations.removeAll()
      return result
    }

    for continuation in continuations {
      continuation.finish()
    }
  }

  /// Creates an independently consumable stream subscribed to this subject.
  public func makeStream() -> AsyncStream<Element> {
    let id = UUID()
    let (stream, continuation) = AsyncStream<Element>.makeStream(
      bufferingPolicy: .bufferingNewest(1)
    )

    continuation.onTermination = { [weak self] _ in
      self?.removeContinuation(id: id)
    }

    let isFinished = state.withLock { state in
      guard !state.isFinished else { return true }
      state.continuations[id] = continuation
      continuation.yield(state.value)
      return false
    }

    if isFinished {
      continuation.finish()
    }

    return stream
  }

  private func removeContinuation(id: UUID) {
    _ = state.withLock { state in
      state.continuations.removeValue(forKey: id)
    }
  }
}
