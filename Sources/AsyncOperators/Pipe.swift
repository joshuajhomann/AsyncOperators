import Foundation
import Synchronization

/// A multicast source that forwards values only to current consumers.
nonisolated public final class Pipe<Element: Sendable>: Sendable {
  typealias AsyncIterator = AsyncStream<Element>.AsyncIterator
  typealias Continuation = AsyncStream<Element>.Continuation

  private struct State: Sendable {
    var continuations: [UUID: Continuation] = [:]
    var isFinished = false
  }

  private let emissions = Mutex<Void>(())
  private let state = Mutex<State>(State())

  public init() {}

  deinit {
    finish()
  }

  /// Sends a value to every consumer that is currently awaiting a value.
  public func send(_ value: Element) {
    emissions.withLock { _ in
      let continuations = state.withLock { state in
        guard !state.isFinished else { return [Continuation]() }
        return Array(state.continuations.values)
      }

      for continuation in continuations {
        continuation.yield(value)
      }
    }
  }

  /// Permanently finishes the pipe and all current consumers.
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

  /// Creates an independently consumable stream subscribed to this pipe.
  public func makeStream() -> AsyncStream<Element> {
    let id = UUID()
    let (stream, continuation) = AsyncStream<Element>.makeStream(
      bufferingPolicy: .bufferingNewest(0)
    )

    continuation.onTermination = { [weak self] _ in
      self?.removeContinuation(id: id)
    }

    emissions.withLock { _ in
      let isFinished = state.withLock { state in
        guard !state.isFinished else { return true }
        state.continuations[id] = continuation
        return false
      }

      if isFinished {
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
