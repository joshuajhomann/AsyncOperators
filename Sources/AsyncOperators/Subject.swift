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

  private let emissions = Mutex<Void>(())
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
    emissions.withLock { _ in
      let continuations = state.withLock { state in
        guard !state.isFinished else { return [Continuation]() }
        state.value = newValue
        return Array(state.continuations.values)
      }

      for continuation in continuations {
        continuation.yield(newValue)
      }
    }
  }

  /// Permanently finishes the subject and all current consumers.
  public func finish() {
    emissions.withLock { _ in
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

    emissions.withLock { _ in
      let initialValue = state.withLock { state -> Element? in
        guard !state.isFinished else { return nil }
        state.continuations[id] = continuation
        return state.value
      }

      if let initialValue {
        continuation.yield(initialValue)
      } else {
        continuation.finish()
      }
    }

    return stream
  }

  private func removeContinuation(id: UUID) {
    _ = state.withLock { state in
      state.continuations.removeValue(forKey: id)
    }
  }
}
