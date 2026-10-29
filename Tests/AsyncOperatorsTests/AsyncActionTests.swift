import AsyncOperators
import Testing

@Suite("AsyncAction")
struct AsyncActionTests {
  @Test("Sequence factories preserve MainActor isolation", arguments: [false, true])
  @MainActor
  func factoryIsolation(externallyEnabled: Bool) async {
    let owner = ActionIsolationOwner()
    let (called, continuation) = AsyncStream<Int>.makeStream()
    let factory: @MainActor @Sendable (Int) -> FixtureSequence = { input in
      MainActor.preconditionIsolated()
      owner.count += input
      continuation.yield(owner.count)
      return FixtureSequence(elements: [], termination: .finished)
    }
    let action =
      externallyEnabled
      ? AsyncAction(enabledIf: Subject(true).makeStream(), workFactory: factory)
      : AsyncAction(workFactory: factory)
    var enabled = action.isEnabled.makeAsyncIterator()
    while await enabled.next() != true {}

    await Task.detached { action.execute(7) }.value
    var iterator = called.makeAsyncIterator()
    #expect(await iterator.next() == 7)
  }

  @Test("Input operations inherit MainActor isolation", arguments: [false, true])
  @MainActor
  func operationIsolation(externallyEnabled: Bool) async {
    let owner = ActionIsolationOwner()
    let (called, continuation) = AsyncStream<Int>.makeStream()
    let action: AsyncAction<Int, Int, Never>
    if externallyEnabled {
      action = AsyncAction(enabledIf: Subject(true).makeStream()) { input in
        await Task.yield()
        MainActor.preconditionIsolated()
        owner.count += input
        continuation.yield(owner.count)
        return owner.count
      }
    } else {
      action = AsyncAction { input in
        await Task.yield()
        MainActor.preconditionIsolated()
        owner.count += input
        continuation.yield(owner.count)
        return owner.count
      }
    }
    var enabled = action.isEnabled.makeAsyncIterator()
    while await enabled.next() != true {}

    await Task.detached { action.execute(7) }.value
    var iterator = called.makeAsyncIterator()
    #expect(await iterator.next() == 7)
  }

  @Test("Inputless throwing operations inherit MainActor isolation")
  @MainActor
  func inputlessIsolation() async {
    let owner = ActionIsolationOwner()
    let (called, continuation) = AsyncStream<Int>.makeStream()
    let action = AsyncAction<Void, Int, FixtureError> {
      () async throws(FixtureError) -> Int in
      await Task.yield()
      MainActor.preconditionIsolated()
      owner.count += 1
      continuation.yield(owner.count)
      throw .expected
    }

    await Task.detached { action.execute() }.value
    var iterator = called.makeAsyncIterator()
    #expect(await iterator.next() == 1)
  }

  @Test("A sequence work factory can produce multiple elements")
  func sequenceWorkFactory() async {
    let action = AsyncAction<String, Int, FixtureError> { input in
      PacedFixtureSequence(elements: input == "numbers" ? [1, 2, 3] : [])
    }
    let stream = action.values
    let values = Task {
      var iterator = stream.makeAsyncIterator()
      var result: [Int] = []

      while result.count < 3, let value = await iterator.next() {
        result.append(value)
      }

      return result
    }

    await Task.yield()
    action.execute("numbers")

    #expect(await values.value == [1, 2, 3])
  }

  @Test("Every values access creates an independent stream")
  func valuesAreMulticast() async {
    let (work, continuation) = AsyncStream<Int>.makeStream(
      bufferingPolicy: .unbounded
    )
    let action = AsyncAction<Void, Int, Never> { _ in work }
    let firstStream = action.values
    let secondStream = action.values

    let first = Task {
      var iterator = firstStream.makeAsyncIterator()
      return await iterator.next()
    }
    let second = Task {
      var iterator = secondStream.makeAsyncIterator()
      return await iterator.next()
    }

    await Task.yield()
    action.execute()
    continuation.yield(42)
    continuation.finish()

    #expect(await first.value == 42)
    #expect(await second.value == 42)
  }

  @Test("Enabled and executing streams contain current action state")
  func stateStreams() async {
    let (work, continuation) = AsyncStream<Int>.makeStream(
      bufferingPolicy: .unbounded
    )
    let action = AsyncAction<Void, Int, Never> { _ in work }
    var enabled = action.isEnabled.makeAsyncIterator()
    var executing = action.isExecuting.makeAsyncIterator()

    #expect(await enabled.next() == true)
    #expect(await executing.next() == false)

    action.execute()

    #expect(await executing.next() == true)
    #expect(await enabled.next() == false)

    continuation.finish()

    #expect(await executing.next() == false)
    #expect(await enabled.next() == true)
  }

  @Test("An externally disabled action ignores execution")
  func externallyDisabled() async {
    let calls = LockedBox(0)
    let enabled = Subject(false)
    let action = AsyncAction<Void, Int, Never>(
      enabledIf: enabled.makeStream(),
      operation: {
        calls.set(calls.value + 1)
        return 1
      }
    )
    var enabledValues = action.isEnabled.makeAsyncIterator()

    #expect(await enabledValues.next() == false)

    action.execute()
    await Task.yield()

    #expect(calls.value == 0)
  }

  @Test("A second concurrent execution is ignored")
  func ignoresConcurrentExecution() async {
    let calls = LockedBox(0)
    let (work, continuation) = AsyncStream<Int>.makeStream(
      bufferingPolicy: .unbounded
    )
    let action = AsyncAction<Void, Int, Never> { _ in
      calls.set(calls.value + 1)
      return work
    }
    var executing = action.isExecuting.makeAsyncIterator()

    #expect(await executing.next() == false)

    action.execute()
    action.execute()

    #expect(await executing.next() == true)

    continuation.finish()

    #expect(await executing.next() == false)
    #expect(calls.value == 1)
  }

  @Test("Deinitializing an action cancels its execution")
  func deinitializationCancelsExecution() async {
    let started = LockedBox(false)
    let cancelled = LockedBox(false)
    var action: AsyncAction<Void, Int, Never>? = AsyncAction {
      await withTaskCancellationHandler {
        started.set(true)
        while !Task.isCancelled {
          await Task.yield()
        }
        return 1
      } onCancel: {
        cancelled.set(true)
      }
    }

    action?.execute()
    for _ in 0..<100 where !started.value {
      await Task.yield()
    }
    #expect(started.value)

    action = nil
    for _ in 0..<100 where !cancelled.value {
      await Task.yield()
    }
    #expect(cancelled.value)
  }

  @Test("Underlying typed failures are sent directly to errors")
  func typedFailure() async {
    let action = AsyncAction<Void, Int, FixtureError> { _ in
      FixtureSequence(elements: [1], termination: .failed)
    }
    let stream = action.errors
    let failure = Task {
      var iterator = stream.makeAsyncIterator()
      return await iterator.next()
    }

    await Task.yield()
    action.execute()

    #expect(await failure.value == .expected)
  }

  @Test("Underlying failures are multicast through independent error streams")
  func errorsAreMulticast() async {
    let action = AsyncAction<Void, Int, FixtureError> { _ in
      FixtureSequence(elements: [], termination: .failed)
    }
    let firstStream = action.errors
    let secondStream = action.errors

    let first = Task {
      var iterator = firstStream.makeAsyncIterator()
      return await iterator.next()
    }
    let second = Task {
      var iterator = secondStream.makeAsyncIterator()
      return await iterator.next()
    }

    await Task.yield()
    action.execute()

    #expect(await first.value == .expected)
    #expect(await second.value == .expected)
  }

  @Test("The async operation overload emits one element")
  func operationOverload() async {
    let action = AsyncAction<Int, String, FixtureError> { input in
      String(input)
    }
    let stream = action.values
    let value = Task {
      var iterator = stream.makeAsyncIterator()
      return await iterator.next()
    }

    await Task.yield()
    let result: Void = action.execute(25)

    #expect(result == ())
    #expect(await value.value == "25")
  }

  @Test("The async operation overload preserves typed failures")
  func throwingOperationOverload() async {
    let action = AsyncAction<Void, Int, FixtureError> {
      () async throws(FixtureError) -> Int in
      throw .expected
    }
    let stream = action.errors
    let failure = Task {
      var iterator = stream.makeAsyncIterator()
      return await iterator.next()
    }

    await Task.yield()
    action.execute()

    #expect(await failure.value == .expected)
  }

  @Test("The inputless operation overload emits one element")
  func inputlessOperationOverload() async {
    let action = AsyncAction<Void, Int, Never> {
      151
    }
    let stream = action.values
    let value = Task {
      var iterator = stream.makeAsyncIterator()
      return await iterator.next()
    }

    await Task.yield()
    action.execute()

    #expect(await value.value == 151)
  }
}

@MainActor
private final class ActionIsolationOwner {
  var count = 0
}

private struct PacedFixtureSequence: AsyncSequence, Sendable {
  struct AsyncIterator: AsyncIteratorProtocol {
    var elements: ArraySlice<Int>

    mutating func next() async throws(FixtureError) -> Int? {
      try? await Task.sleep(for: .milliseconds(5))
      return elements.popFirst()
    }
  }

  let elements: [Int]

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(elements: elements[...])
  }
}
