import Synchronization

/// Combines any number of sendable sequences into a flat tuple of latest values.
///
/// Iteration starts on the first call to `next()`. No tuple is emitted until all
/// inputs have produced a value. An empty input ends the combination immediately;
/// otherwise completed inputs retain their last value until every input finishes.
/// An upstream error discards buffered tuples and cancels the other inputs.
/// Zero inputs produce an empty sequence; one input produces its elements directly.
nonisolated public func combineLatest<each Base>(
  _ bases: repeat each Base
) -> AsyncCombineLatestSequence<repeat each Base>
where repeat each Base: AsyncSequence & Sendable, repeat (each Base).Element: Sendable {
  AsyncCombineLatestSequence(repeat each bases)
}

/// A variadic combination whose element is a flat tuple in input order.
///
/// Different upstream failure types are exposed as `any Error`. Each iterator
/// independently iterates its inputs, subject to those inputs' own sharing rules.
nonisolated public struct AsyncCombineLatestSequence<each Base>: AsyncSequence, Sendable
where repeat each Base: AsyncSequence & Sendable, repeat (each Base).Element: Sendable {
  public typealias Element = (repeat (each Base).Element)
  public typealias Failure = any Error

  private let bases: (repeat each Base)

  fileprivate init(_ bases: repeat each Base) {
    self.bases = (repeat each bases)
  }

  public func makeAsyncIterator() -> Iterator {
    var sources: [LatestStorage.Source] = []
    for base in repeat each bases {
      sources.append { storage, index in
        await storage.consume(base, at: index)
      }
    }
    return Iterator(storage: LatestStorage(sources: sources))
  }

  public struct Iterator: AsyncIteratorProtocol {
    private let lifetime: LatestIteratorLifetime

    fileprivate init(storage: LatestStorage) {
      lifetime = LatestIteratorLifetime(storage: storage)
    }

    public mutating func next(
      isolation actor: isolated (any Actor)? = #isolation
    ) async throws -> Element? {
      guard let values = try await lifetime.storage.next() else { return nil }
      var index = 0
      func unpack<Value>(_ type: Value.Type) -> Value {
        defer { index += 1 }
        // Sources and tuple elements are generated from the same pack in the
        // same order. Only source i can write slot i; its type cannot change.
        return values[index] as! Value
      }
      return (repeat unpack((each Base).Element.self))
    }
  }
}

@available(*, unavailable)
extension AsyncCombineLatestSequence.Iterator: Sendable {}

nonisolated private final class LatestIteratorLifetime {
  let storage: LatestStorage

  init(storage: LatestStorage) {
    self.storage = storage
  }

  deinit {
    storage.cancel()
  }
}

/// One mutex orders updates, demand, completion, and cancellation for all inputs.
nonisolated private final class LatestStorage: Sendable {
  typealias Source = @Sendable (LatestStorage, Int) async -> Void
  typealias Snapshot = [any Sendable]
  typealias Downstream = CheckedContinuation<Result<Snapshot?, any Error>, Never>

  private struct State: Sendable {
    var latest: [(any Sendable)?]
    var missing: Int
    var remaining: Int
    var started = false
    var finished = false
    var error: (any Error)?
    var buffer: [Snapshot] = []
    var downstream: Downstream?
    var upstreams: [Int: CheckedContinuation<Bool, Never>] = [:]
    var task: Task<Void, Never>?

    // Cancellation must happen outside the mutex: task cancellation handlers
    // may synchronously call back into the storage.
    mutating func finish(
      error: (any Error)? = nil,
      discardBuffer: Bool = false
    ) -> Task<Void, Never>? {
      finished = true
      self.error = error
      if discardBuffer { buffer.removeAll() }
      latest.removeAll()
      if let downstream {
        self.downstream = nil
        downstream.resume(returning: error.map { .failure($0) } ?? .success(nil))
        self.error = nil
      }
      for continuation in upstreams.values {
        continuation.resume(returning: false)
      }
      upstreams.removeAll()
      let task = self.task
      self.task = nil
      return task
    }
  }

  private let sources: [Source]
  private let state: Mutex<State>

  init(sources: [Source]) {
    self.sources = sources
    state = Mutex(
      State(
        latest: Array(repeating: nil, count: sources.count),
        missing: sources.count,
        remaining: sources.count,
        finished: sources.isEmpty
      )
    )
  }

  nonisolated(nonsending) func next() async throws -> Snapshot? {
    let result: Result<Snapshot?, any Error> = await withTaskCancellationHandler {
      await withCheckedContinuation { continuation in
        state.withLock { state in
          precondition(state.downstream == nil, "Concurrent next() calls are unsupported")
          if let error = state.error {
            state.error = nil
            continuation.resume(returning: .failure(error))
          } else if !state.buffer.isEmpty {
            continuation.resume(returning: .success(state.buffer.removeFirst()))
          } else if state.finished {
            continuation.resume(returning: .success(nil))
          } else {
            state.downstream = continuation
            if !state.started {
              state.started = true
              state.task = Task {
                await withTaskGroup(of: Void.self) { group in
                  for (index, source) in sources.enumerated() {
                    group.addTask { await source(self, index) }
                  }
                }
              }
            }
            for upstream in state.upstreams.values {
              upstream.resume(returning: true)
            }
            state.upstreams.removeAll()
          }
        }
      }
    } onCancel: {
      self.cancel()
    }
    return try result.get()
  }

  func cancel() {
    let task = state.withLock { $0.finish(discardBuffer: true) }
    task?.cancel()
  }

  @concurrent
  func consume<Base: AsyncSequence & Sendable>(_ base: Base, at index: Int) async
  where Base.Element: Sendable {
    var iterator = base.makeAsyncIterator()
    do {
      while await withCheckedContinuation({ continuation in
        state.withLock { state in
          if state.finished {
            continuation.resume(returning: false)
          } else if state.downstream != nil {
            continuation.resume(returning: true)
          } else {
            state.upstreams[index] = continuation
          }
        }
      }) {
        guard let element = try await iterator.next(isolation: nil) else {
          let task = state.withLock { state -> Task<Void, Never>? in
            guard !state.finished else { return nil }
            state.remaining -= 1
            if state.latest[index] == nil || state.remaining == 0 {
              return state.finish()
            }
            return nil
          }
          task?.cancel()
          return
        }
        state.withLock { state in
          guard !state.finished else { return }
          if state.latest[index] == nil { state.missing -= 1 }
          state.latest[index] = .some(element)
          guard state.missing == 0 else { return }
          let snapshot = state.latest.map { $0! }
          if let downstream = state.downstream {
            state.downstream = nil
            downstream.resume(returning: .success(snapshot))
          } else {
            // Only reads already in flight can arrive without demand. Each
            // source suspends before its next read, bounding this buffer.
            state.buffer.append(snapshot)
          }
        }
      }
    } catch {
      let task = state.withLock { state -> Task<Void, Never>? in
        guard !state.finished else { return nil }
        return state.finish(error: error, discardBuffer: true)
      }
      task?.cancel()
    }
  }
}
