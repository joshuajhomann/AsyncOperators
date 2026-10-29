/// The reason an async-sequence subscription stopped.
public enum AsyncSequenceCompletion<Failure: Error> {
  /// The sequence reached its natural end.
  case finished

  /// The subscription task was cancelled.
  case cancelled

  /// The sequence terminated by throwing an error.
  case error(Failure)
}

extension AsyncSequenceCompletion: Equatable where Failure: Equatable {}
extension AsyncSequenceCompletion: Sendable where Failure: Sendable {}
