import Synchronization

/// Serializes executions of asynchronous work and aggregates their events.
nonisolated public final class AsyncAction<
  Input: Sendable,
  Element: Sendable,
  Failure: Error & Sendable
>: Sendable {
  private typealias Receiver = @Sendable (Element) -> Void
  private typealias Execution =
    @Sendable (
      Input,
      Receiver
    ) async throws(Failure) -> Void

  private struct State: Sendable {
    var externallyEnabled: Bool
    var isExecuting = false
    var executionGeneration: UInt = 0
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
    state = Mutex(State(externallyEnabled: initiallyEnabled))
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

  /// Creates an action that is enabled whenever it is not executing.
  public convenience init<Work: AsyncSequence & Sendable>(
    @_inheritActorContext workFactory: @escaping @isolated(any) @Sendable (Input) -> Work
  ) where Work.Element == Element, Work.Failure == Failure {
    self.init(
      initiallyEnabled: true,
      execution: Self.makeExecution(workFactory: workFactory)
    )
  }

  /// Creates an action whose availability also follows `enabledIf`.
  public convenience init<Enabled, Work>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled,
    @_inheritActorContext workFactory: @escaping @isolated(any) @Sendable (Input) -> Work
  )
  where
    Enabled: AsyncSequence & Sendable,
    Enabled.Element == Bool,
    Enabled.Failure == Never,
    Work: AsyncSequence & Sendable,
    Work.Element == Element,
    Work.Failure == Failure
  {
    self.init(
      initiallyEnabled: false,
      execution: Self.makeExecution(workFactory: workFactory)
    )
    observe(isolation: isolation, enabledIf: enabledIf)
  }

  /// Creates a one-element action from an asynchronous operation.
  public convenience init(
    @_inheritActorContext operation: @escaping @isolated(any) @Sendable (Input) async throws(Failure) -> Element
  ) {
    self.init(
      initiallyEnabled: true,
      execution: Self.makeExecution(operation: operation)
    )
  }

  /// Creates an externally enabled, one-element action.
  public convenience init<Enabled: AsyncSequence & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled,
    @_inheritActorContext operation: @escaping @isolated(any) @Sendable (Input) async throws(Failure) -> Element
  ) where Enabled.Element == Bool, Enabled.Failure == Never {
    self.init(
      initiallyEnabled: false,
      execution: Self.makeExecution(operation: operation)
    )
    observe(isolation: isolation, enabledIf: enabledIf)
  }

  /// Begins an execution immediately when the action is enabled and idle.
  public func execute(_ input: Input) {
    let generation = state.withLock { state -> UInt? in
      guard !state.isExecuting, state.externallyEnabled else { return nil }
      state.executionGeneration &+= 1
      state.isExecuting = true
      executingSubject.send(true)
      enabledSubject.send(false)
      return state.executionGeneration
    }

    guard let generation else { return }
    executionSubscription.cancelAll()

    let execution = self.execution
    let valuesPipe = self.valuesPipe
    let errorsPipe = self.errorsPipe

    let task = Task { [weak self] in
      defer { self?.completeExecution(generation: generation) }

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

    let executionAlreadyCompleted = state.withLock { state in
      guard
        state.isExecuting,
        state.executionGeneration == generation
      else { return true }
      executionSubscription += task
      return false
    }

    if executionAlreadyCompleted {
      task.cancel()
    }
  }

  private static func makeExecution<Work: AsyncSequence & Sendable>(
    workFactory: @escaping @isolated(any) @Sendable (Input) -> Work
  ) -> Execution where Work.Element == Element, Work.Failure == Failure {
    { (input, receive) async throws(Failure) in
      for try await element in await workFactory(input) {
        guard !Task.isCancelled else { return }
        receive(element)
      }
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
    makeExecution { (_: Input) async throws(Failure) -> Element in
      try await operation()
    }
  }

  private func observe<Enabled: AsyncSequence & Sendable>(
    isolation: isolated (any Actor)?,
    enabledIf: Enabled
  ) where Enabled.Element == Bool, Enabled.Failure == Never {
    subscriptions += enabledIf.subscribe(isolation: isolation) { [weak self] enabled in
      guard let self else { return }
      state.withLock { state in
        state.externallyEnabled = enabled
        enabledSubject.send(enabled && !state.isExecuting)
      }
    }
  }

  private func completeExecution(generation: UInt) {
    state.withLock { state in
      guard
        state.isExecuting,
        state.executionGeneration == generation
      else { return }
      state.isExecuting = false
      executingSubject.send(false)
      enabledSubject.send(state.externallyEnabled)
    }
  }
}

extension AsyncAction where Input == Void {
  /// Begins an inputless execution immediately.
  public func execute() {
    execute(())
  }

  /// Creates a one-element, inputless action.
  public convenience init(
    @_inheritActorContext _ operation:
      @escaping @isolated(any) @Sendable () async throws(Failure) -> Element
  ) {
    self.init(
      initiallyEnabled: true,
      execution: Self.makeExecution(operation: operation)
    )
  }

  /// Creates an externally enabled, one-element, inputless action.
  public convenience init<Enabled: AsyncSequence & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled,
    @_inheritActorContext _ operation:
      @escaping @isolated(any) @Sendable () async throws(Failure) -> Element
  ) where Enabled.Element == Bool, Enabled.Failure == Never {
    self.init(
      initiallyEnabled: false,
      execution: Self.makeExecution(operation: operation)
    )
    observe(isolation: isolation, enabledIf: enabledIf)
  }
}

extension AsyncAction where Failure == Never {
  /// Creates a one-element action from a nonthrowing asynchronous operation.
  public convenience init(
    @_inheritActorContext operation: @escaping @isolated(any) @Sendable (Input) async -> Element
  ) {
    self.init(
      initiallyEnabled: true,
      execution: Self.makeExecution(operation: operation)
    )
  }

  /// Creates an externally enabled action from a nonthrowing operation.
  public convenience init<Enabled: AsyncSequence & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled,
    @_inheritActorContext operation: @escaping @isolated(any) @Sendable (Input) async -> Element
  ) where Enabled.Element == Bool, Enabled.Failure == Never {
    self.init(
      initiallyEnabled: false,
      execution: Self.makeExecution(operation: operation)
    )
    observe(isolation: isolation, enabledIf: enabledIf)
  }
}

extension AsyncAction where Input == Void, Failure == Never {
  /// Creates a one-element, inputless, nonthrowing action.
  public convenience init(
    @_inheritActorContext _ operation: @escaping @isolated(any) @Sendable () async -> Element
  ) {
    self.init(
      initiallyEnabled: true,
      execution: Self.makeExecution(operation: operation)
    )
  }

  /// Creates an externally enabled, inputless, nonthrowing action.
  public convenience init<Enabled: AsyncSequence & Sendable>(
    isolation: isolated (any Actor)? = #isolation,
    enabledIf: Enabled,
    @_inheritActorContext _ operation: @escaping @isolated(any) @Sendable () async -> Element
  ) where Enabled.Element == Bool, Enabled.Failure == Never {
    self.init(
      initiallyEnabled: false,
      execution: Self.makeExecution(operation: operation)
    )
    observe(isolation: isolation, enabledIf: enabledIf)
  }
}
