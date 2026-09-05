import AsyncOperators
import Testing

@Suite("AsyncSequence subscriptions")
struct SubscribeTests {
  @Test("Nonthrowing subscribe consumes every value")
  func consumesNonthrowingSequence() async {
    let (stream, continuation) = AsyncStream<Int>.makeStream(
      bufferingPolicy: .unbounded
    )
    let received = Recorder<Int>()
    let task = stream.subscribe { value in
      received.append(value)
    }

    continuation.yield(1)
    continuation.yield(2)
    continuation.yield(3)
    continuation.finish()
    await task.value

    #expect(received.values == [1, 2, 3])
  }

  @Test("Completion subscribe reports natural completion")
  func reportsFinishedCompletion() async {
    let sequence = FixtureSequence(
      elements: [1, 2],
      termination: .finished
    )
    let completion = LockedBox<AsyncSequenceCompletion<FixtureError>?>(nil)
    let received = Recorder<Int>()

    let task = sequence.subscribe(
      onComplete: { completion.set($0) },
      { received.append($0) }
    )
    await task.value

    #expect(received.values == [1, 2])
    #expect(completion.value == .finished)
  }

  @Test("Completion subscribe reports typed errors")
  func reportsErrorCompletion() async {
    let sequence = FixtureSequence(elements: [1], termination: .failed)
    let completion = LockedBox<AsyncSequenceCompletion<FixtureError>?>(nil)

    let task = sequence.subscribe(
      onComplete: { completion.set($0) },
      { _ in }
    )
    await task.value

    #expect(completion.value == .error(.expected))
  }

  @Test("Completion subscribe distinguishes cancellation")
  func reportsCancellation() async {
    let stream = AsyncStream<Int> { _ in }
    let wasCancelled = LockedBox(false)
    let task = stream.subscribe(
      onComplete: { completion in
        if case .cancelled = completion {
          wasCancelled.set(true)
        }
      },
      { _ in }
    )

    task.cancel()
    await task.value

    #expect(wasCancelled.value)
  }

  @Test("Referencing subscribe does not retain its consumer")
  func referencingSubscriptionIsWeak() async {
    final class Consumer: Sendable {
      let values = Recorder<Int>()
    }

    let (stream, continuation) = AsyncStream<Int>.makeStream(
      bufferingPolicy: .unbounded
    )
    var consumer: Consumer? = Consumer()
    weak let weakConsumer = consumer

    let task = stream.subscribe(referencing: consumer!) { consumer, value in
      consumer.values.append(value)
    }
    consumer = nil

    #expect(weakConsumer == nil)

    continuation.yield(1)
    continuation.finish()
    await task.value
  }

  @Test("Assign maps tuple elements to matching key paths")
  func assignsTupleElements() async {
    final class Target: @unchecked Sendable {
      var count = 0
      var name = ""
    }

    let (stream, continuation) = AsyncStream<(Int, String)>.makeStream(
      bufferingPolicy: .unbounded
    )
    let target = Target()
    let task = stream.assign(
      on: target,
      to: \Target.count,
      \Target.name
    )

    continuation.yield((42, "Pikachu"))
    continuation.finish()
    await task.value

    #expect(target.count == 42)
    #expect(target.name == "Pikachu")
  }
}
