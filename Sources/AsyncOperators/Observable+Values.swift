import Observation

extension Observation.Observable where Self: AnyObject & Sendable {
  /// Produces the current value and every subsequently observed value.
  public func values<Value: Sendable>(
    of keyPath: KeyPath<Self, Value> & Sendable,
    isolation: isolated (any Actor)? = #isolation
  ) -> some AsyncSequence<Value, Never> {
    _ = isolation

    return Observations.untilFinished { [weak self] in
      guard let self else { return .finish }
      return .next(self[keyPath: keyPath])
    }
  }

  /// Produces observed values after the current value.
  public func newValues<Value: Sendable>(
    of keyPath: KeyPath<Self, Value> & Sendable,
    isolation: isolated (any Actor)? = #isolation
  ) -> some AsyncSequence<Value, Never> {
    values(of: keyPath, isolation: isolation).dropFirst()
  }
}
