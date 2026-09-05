import Observation

private final class ObservableValuesCapture<Value: Sendable>: @unchecked Sendable {
  private let produceIteration: () -> Observations<Value, Never>.Iteration

  init<Object: AnyObject>(object: Object, keyPath: KeyPath<Object, Value>) {
    produceIteration = { [weak object] in
      guard let object else { return .finish }
      return .next(object[keyPath: keyPath])
    }
  }

  func next() -> Observations<Value, Never>.Iteration {
    produceIteration()
  }
}

extension Observation.Observable where Self: AnyObject {
  /// Produces the current value and every subsequently observed value.
  public func values<Value: Sendable>(
    of keyPath: KeyPath<Self, Value>,
    isolation: isolated (any Actor)? = #isolation
  ) -> some AsyncSequence<Value, Never> {
    _ = isolation
    let capture = ObservableValuesCapture(object: self, keyPath: keyPath)

    return Observations.untilFinished {
      capture.next()
    }
  }

  /// Produces observed values after the current value.
  public func newValues<Value: Sendable>(
    of keyPath: KeyPath<Self, Value>,
    isolation: isolated (any Actor)? = #isolation
  ) -> some AsyncSequence<Value, Never> {
    values(of: keyPath, isolation: isolation).dropFirst()
  }
}
