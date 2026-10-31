import Synchronization

/// Serializes executions of asynchronous work and aggregates their events.
nonisolated public final class AsyncAction<
  Input: Sendable,
  Element: Sendable,
  Failure: Error & Sendable
>: Sendable {
  private typealias Execution =
    @Sendable (Input, @Sendable (Element) -> Void) async throws(Failure) -> Void

  private struct State: Sendable {
    var isExternallyEnabled: Bool
    var isExecuting = false
  }

  private let execution: Execution
  private let state: Mutex<State>
  private let subscriptions = Subscriptions()
  private let executionSubscription = Subscriptions()

  private let valuesPipe = Pipe<Element>()
  private let errorsPipe = Pipe<Failure>()
  private let enabledSubject: Subject<Bool>
  private let executingSubject = Subject(false)

  /// Successful elements aggregated from every execution.
  public var values: AsyncStream<Element> {
    valuesPipe.makeStream()
  }

  /// Underlying failures aggregated from every execution.
  public var errors: AsyncStream<Failure> {
    errorsPipe.makeStream()
  }

  /// The current enabled state followed by subsequent changes.
  public var isEnabled: AsyncStream<Bool> {
    enabledSubject.makeStream()
  }

  /// The current execution state followed by subsequent changes.
  public var isExecuting: AsyncStream<Bool> {
    executingSubject.makeStream()
  }

  private init(
    initiallyEnabled: Bool,
    execution: @escaping Execution
  ) {
    self.execution = execution
    state = Mutex(State(isExternallyEnabled: initiallyEnabled))
    enabledSubject = Subject(initiallyEnabled)
  }

  deinit {
    executionSubscription.cancelAll()
    subscriptions.cancelAll()
    valuesPipe.finish()
    errorsPipe.finish()
    enabledSubject.finish()
    executingSubject.finish()
  }

  /// Begins an execution immediately when the action is enabled and idle.
  public func execute(_ input: Input) {
    let shouldExecute = state.withLock { state in
      guard !state.isExecuting, state.isExternallyEnabled else { return false }
      state.isExecuting = true
      executingSubject.send(true)
      enabledSubject.send(false)
      return true
    }

    guard shouldExecute else { return }
    executionSubscription.cancelAll()

    let execution = self.execution
    let valuesPipe = self.valuesPipe
    let errorsPipe = self.errorsPipe

    // Register under the state lock so completion cannot make the action idle first.
    state.withLock { _ in
      executionSubscription += Task { [weak self] in
        defer { self?.completeExecution() }

        do throws(Failure) {
          try await execution(input) { element in
            guard !Task.isCancelled else { return }
            valuesPipe.send(element)
          }
        } catch {
          guard !Task.isCancelled else { return }
          errorsPipe.send(error)
        }
      }
    }
  }

  private func observe<Enabled: AsyncSequence<Bool, Never> & Sendable>(
    isolation: isolated (any Actor)?,
    enabledIf: Enabled
  ) {
    subscriptions += enabledIf.subscribe(isolation: isolation) { [weak self] enabled in
      guard let self else { return }
      state.withLock { state in
        state.isExternallyEnabled = enabled
        enabledSubject.send(enabled && !state.isExecuting)
      }
    }
  }

  private func completeExecution() {
    state.withLock { state in
      state.isExecuting = false
      executingSubject.send(false)
      enabledSubject.send(state.isExternallyEnabled)
    }
  }
}

extension AsyncAction {
  /// Creates an action that is enabled by default, or follows `enabledIf` when supplied.
  public convenience init<Enabled: AsyncSequence<Bool, Never> & Sendable, Work>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled? = Optional<AsyncStream<Bool>>.none,
    @_inheritActorContext workFactory: @escaping @isolated(any) @Sendable (Input) -> Work
  )
  where
    Work: AsyncSequence & Sendable,
    Work.Element == Element,
    Work.Failure == Failure
  {
    self.init(
      initiallyEnabled: enabledIf == nil,
      execution: { (input, receive) async throws(Failure) in
        for try await element in await workFactory(input) {
          guard !Task.isCancelled else { return }
          receive(element)
        }
      }
    )
    if let enabledIf {
      observe(isolation: isolation, enabledIf: enabledIf)
    }
  }

  /// Creates a one-element action, optionally controlled by `enabledIf`.
  public convenience init<Enabled: AsyncSequence<Bool, Never> & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled? = Optional<AsyncStream<Bool>>.none,
    @_inheritActorContext operation:
      @escaping @isolated(any) @Sendable (Input) async throws(Failure) -> Element
  ) {
    self.init(
      initiallyEnabled: enabledIf == nil,
      execution: Self.makeExecution(operation: operation)
    )
    if let enabledIf {
      observe(isolation: isolation, enabledIf: enabledIf)
    }
  }

  private static func makeExecution(
    operation:
      @escaping @isolated(any) @Sendable (Input) async throws(Failure) -> Element
  ) -> Execution {
    { (input, receive) async throws(Failure) in
      receive(try await operation(input))
    }
  }

  private static func makeExecution(
    operation:
      @escaping @isolated(any) @Sendable () async throws(Failure) -> Element
  ) -> Execution {
    { (_, receive) async throws(Failure) in
      receive(try await operation())
    }
  }
}

extension AsyncAction where Input == Void {
  /// Begins an inputless execution immediately.
  public func execute() {
    execute(())
  }

  /// Creates a one-element, inputless action, optionally controlled by `enabledIf`.
  public convenience init<Enabled: AsyncSequence<Bool, Never> & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled? = Optional<AsyncStream<Bool>>.none,
    @_inheritActorContext _ operation:
      @escaping @isolated(any) @Sendable () async throws(Failure) -> Element
  ) {
    self.init(
      initiallyEnabled: enabledIf == nil,
      execution: Self.makeExecution(operation: operation)
    )
    if let enabledIf {
      observe(isolation: isolation, enabledIf: enabledIf)
    }
  }
}

extension AsyncAction where Failure == Never {
  /// Creates a nonthrowing action, optionally controlled by `enabledIf`.
  public convenience init<Enabled: AsyncSequence<Bool, Never> & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled? = Optional<AsyncStream<Bool>>.none,
    @_inheritActorContext operation: @escaping @isolated(any) @Sendable (Input) async -> Element
  ) {
    self.init(
      initiallyEnabled: enabledIf == nil,
      execution: Self.makeExecution(operation: operation)
    )
    if let enabledIf {
      observe(isolation: isolation, enabledIf: enabledIf)
    }
  }
}

extension AsyncAction where Input == Void, Failure == Never {
  /// Creates an inputless, nonthrowing action, optionally controlled by `enabledIf`.
  public convenience init<Enabled: AsyncSequence<Bool, Never> & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled? = Optional<AsyncStream<Bool>>.none,
    @_inheritActorContext _ operation: @escaping @isolated(any) @Sendable () async -> Element
  ) {
    self.init(
      initiallyEnabled: enabledIf == nil,
      execution: Self.makeExecution(operation: operation)
    )
    if let enabledIf {
      observe(isolation: isolation, enabledIf: enabledIf)
    }
  }
}
