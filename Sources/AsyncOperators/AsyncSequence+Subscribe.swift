extension AsyncSequence where Element: Sendable {
  /// Consumes this sequence in a task that inherits the caller's isolation.
  ///
  /// The completion closure is called exactly once when iteration finishes,
  /// observes cancellation, or throws.
  @discardableResult
  public func subscribe(
    isolation: isolated (any Actor)? = #isolation,
    onComplete: @escaping (AsyncSequenceCompletion<Failure>) -> Void,
    _ perform: @escaping (Element) async -> Void
  ) -> Task<Void, Never> {
    Task {
      _ = isolation

      do throws(Failure) {
        for try await element in self {
          guard !Task.isCancelled else {
            onComplete(.cancelled)
            return
          }
          await perform(element)
        }

        onComplete(Task.isCancelled ? .cancelled : .finished)
      } catch {
        onComplete(Task.isCancelled ? .cancelled : .error(error))
      }
    }
  }
}

extension AsyncSequence where Failure == Never, Element: Sendable {
  /// Consumes every value from this nonthrowing sequence in a task that
  /// inherits the caller's isolation.
  @discardableResult
  public func subscribe(
    isolation: isolated (any Actor)? = #isolation,
    _ perform: @escaping (Element) async -> Void
  ) -> Task<Void, Never> {
    Task {
      _ = isolation

      for await element in self {
        guard !Task.isCancelled else { return }
        await perform(element)
      }
    }
  }

  /// Consumes values without retaining the object passed to `referencing`.
  /// The subscription ends as soon as that object is released.
  @discardableResult
  public func subscribe<Reference: AnyObject>(
    isolation: isolated (any Actor)? = #isolation,
    referencing reference: Reference,
    _ perform: @escaping (Reference, Element) -> Void
  ) -> Task<Void, Never> {
    Task { [weak reference] in
      _ = isolation

      for await element in self {
        guard !Task.isCancelled, let reference else { return }
        perform(reference, element)
      }
    }
  }

  /// Assigns tuple elements to the corresponding writable key paths.
  @discardableResult
  public func assign<each Value, Target: AnyObject>(
    isolation: isolated (any Actor)? = #isolation,
    on target: Target,
    to keyPaths: repeat ReferenceWritableKeyPath<Target, each Value>
  ) -> Task<Void, Never> where Element == (repeat each Value) {
    subscribe(isolation: isolation, referencing: target) { target, values in
      for (value, keyPath) in repeat (each values, each keyPaths) {
        target[keyPath: keyPath] = value
      }
    }
  }
}
