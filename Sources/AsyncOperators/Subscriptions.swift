import Synchronization

/// Owns subscription tasks and cancels them together or on deinitialization.
public final class Subscriptions: Sendable {
  private let cancellations = Mutex<[@Sendable () -> Void]>([])

  public init() {}

  deinit {
    cancelAll()
  }

  /// Adds a task to this collection.
  public func insert<Success, Failure: Error>(
    _ task: Task<Success, Failure>
  ) {
    cancellations.withLock { cancellations in
      cancellations.append { task.cancel() }
    }
  }

  /// Cancels every stored task and releases the stored cancellation closures.
  public func cancelAll() {
    let cancellations = cancellations.withLock { cancellations in
      let result = cancellations
      cancellations.removeAll()
      return result
    }

    for cancel in cancellations {
      cancel()
    }
  }

  public static func += <Success, Failure: Error>(
    lhs: Subscriptions,
    rhs: Task<Success, Failure>
  ) {
    lhs.insert(rhs)
  }
}
