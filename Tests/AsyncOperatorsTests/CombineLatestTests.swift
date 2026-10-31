import AsyncAlgorithms
import AsyncOperators
import Testing

@Suite("Variadic combineLatest", .timeLimit(.minutes(1)))
struct CombineLatestTests {
  @Test("Actor-isolated consumers can use the modern iterator requirement")
  @MainActor
  func actorIsolatedIteration() async throws {
    var iterator = AsyncOperators.combineLatest(
      [1].async, ["a"].async, [true].async, [2.5].async
    ).makeAsyncIterator()
    let value = try #require(await iterator.next(isolation: MainActor.shared))
    MainActor.preconditionIsolated()
    #expect(value == (1, "a", true, 2.5))
    #expect(try await iterator.next(isolation: MainActor.shared) == nil)
  }

  @Test("Zero inputs finish immediately")
  func zeroInputs() async throws {
    var iterator = AsyncOperators.combineLatest().makeAsyncIterator()
    #expect(try await iterator.next() == nil)
  }

  @Test("One input preserves every element")
  func oneInput() async throws {
    var values: [Int] = []
    for try await value in AsyncOperators.combineLatest([1, 2, 3].async) {
      values.append(value)
    }
    #expect(values == [1, 2, 3])
  }

  @Test("Four heterogeneous inputs yield flat tuples and independent updates")
  func fourInputs() async throws {
    let (numbers, number) = AsyncStream<Int>.makeStream()
    let (names, name) = AsyncStream<String>.makeStream()
    let (flags, flag) = AsyncStream<Bool>.makeStream()
    let (scores, score) = AsyncStream<Double>.makeStream()
    var iterator = combineLatest(numbers, names, flags, scores).makeAsyncIterator()
    number.yield(1)
    name.yield("one")
    flag.yield(false)
    score.yield(1.5)

    let first = try #require(await iterator.next())
    #expect(first == (1, "one", false, 1.5))

    name.yield("two")
    let second = try #require(await iterator.next())
    #expect(second == (1, "two", false, 1.5))

    flag.yield(true)
    let third = try #require(await iterator.next())
    #expect(third == (1, "two", true, 1.5))

    number.finish()
    name.finish()
    flag.finish()
    score.finish()
    #expect(try await iterator.next() == nil)
  }

  @Test("Optional nil is a value, not an input that has not emitted")
  func optionalElements() async throws {
    var iterator = AsyncOperators.combineLatest(
      [Int?.none].async, ["name"].async, [Bool?.none].async, [4].async
    ).makeAsyncIterator()
    let value = try #require(await iterator.next())
    #expect(value.0 == nil)
    #expect(value.1 == "name")
    #expect(value.2 == nil)
    #expect(value.3 == 4)
    #expect(try await iterator.next() == nil)
  }

  @Test("Waits for every first value and keeps the latest early update")
  func waitsForAllInputs() async throws {
    let fast = Probe<Int>()
    let slow = Probe<String>()
    let consumer = Task {
      var iterator = AsyncOperators.combineLatest(fast.sequence, slow.sequence).makeAsyncIterator()
      return try await iterator.next()
    }
    await fast.waitForRead()
    fast.continuation.yield(1)
    await fast.waitForRead()
    fast.continuation.yield(2)
    await fast.waitForRead()
    slow.continuation.yield("ready")
    let value = try #require(await consumer.value)
    #expect(value == (2, "ready"))
  }

  @Test("Cancellation before first next starts no upstream work")
  func cancelledBeforeFirstNext() async throws {
    let reads = Recorder<Int>()
    let (gate, start) = AsyncStream<Void>.makeStream()
    let consumer = Task {
      var gateIterator = gate.makeAsyncIterator()
      _ = await gateIterator.next()
      var iterator = AsyncOperators.combineLatest(
        CountingSequence(reads: reads), CountingSequence(reads: reads)
      ).makeAsyncIterator()
      return try await iterator.next()
    }
    consumer.cancel()
    start.finish()
    #expect(try await consumer.value == nil)
    #expect(reads.values.isEmpty)
  }

  @Test("Completed inputs retain their final values")
  func retainsCompletedValues() async throws {
    let (updates, continuation) = AsyncStream<Int>.makeStream()
    var iterator = AsyncOperators.combineLatest(
      ["fixed"].async, updates, [true].async, [2.5].async
    ).makeAsyncIterator()
    continuation.yield(1)
    #expect(try await iterator.next()?.1 == 1)
    continuation.yield(2)
    #expect(try await iterator.next()?.1 == 2)
    continuation.finish()
    #expect(try await iterator.next() == nil)
  }

  @Test("Independent iterators do not share latest values")
  func independentIterators() async throws {
    let combined = AsyncOperators.combineLatest([1].async, ["a"].async, [true].async, [2].async)
    var first = combined.makeAsyncIterator()
    var second = combined.makeAsyncIterator()
    #expect(try await first.next()?.0 == 1)
    #expect(try await first.next() == nil)
    #expect(try await second.next()?.0 == 1)
    #expect(try await second.next() == nil)
  }

  @Test("No upstream work starts before next")
  func lazyStart() {
    let reads = Recorder<Int>()
    let probe = CountingSequence(reads: reads)
    _ = AsyncOperators.combineLatest(probe, probe, probe, probe).makeAsyncIterator()
    #expect(reads.values.isEmpty)
  }

  @Test("Slow consumers do not cause unbounded upstream reads")
  func boundedReads() async throws {
    let reads = Recorder<Int>()
    let probe = CountingSequence(reads: reads)
    var iterator = AsyncOperators.combineLatest(probe, probe, probe, probe).makeAsyncIterator()
    _ = try await iterator.next()
    // Let any already-started upstream reads finish while demand is absent.
    for _ in 0..<100 { await Task.yield() }
    let count = reads.values.count
    for _ in 0..<100 { await Task.yield() }
    #expect(reads.values.count == count)
  }

  @Test("An empty input cancels siblings waiting for values")
  func emptyCancelsSiblings() async throws {
    let empty = Probe<Int>()
    let sibling = Probe<String>()
    let consumer = Task {
      var iterator = AsyncOperators.combineLatest(empty.sequence, sibling.sequence)
        .makeAsyncIterator()
      return try await iterator.next()
    }
    await empty.waitForRead()
    await sibling.waitForRead()
    empty.continuation.finish()
    #expect(try await consumer.value == nil)
    await sibling.waitForTermination()
  }

  @Test("An upstream failure reaches the consumer and cancels siblings")
  func failureCancelsSiblings() async throws {
    let failing = Probe<Int>()
    let sibling = Probe<String>()
    let consumer = Task<Void, any Error> { () async throws -> Void in
      var iterator = AsyncOperators.combineLatest(failing.sequence, sibling.sequence)
        .makeAsyncIterator()
      do {
        _ = try await iterator.next()
        Issue.record("Expected upstream failure")
      } catch {
        #expect(error as? FixtureError == .expected)
      }
      let finished = try await iterator.next()
      #expect(finished == nil)
    }
    await failing.waitForRead()
    await sibling.waitForRead()
    failing.continuation.finish(throwing: FixtureError.expected)
    try await consumer.value
    await sibling.waitForTermination()
  }

  @Test("Cancelling downstream finishes next and cancels all inputs")
  func cancellation() async throws {
    let first = Probe<Int>()
    let second = Probe<String>()
    let consumer = Task {
      var iterator = AsyncOperators.combineLatest(first.sequence, second.sequence)
        .makeAsyncIterator()
      return try await iterator.next()
    }
    await first.waitForRead()
    await second.waitForRead()
    consumer.cancel()
    #expect(try await consumer.value == nil)
    await first.waitForTermination()
    await second.waitForTermination()
  }

  @Test("Failure takes precedence over pending updates when the consumer pauses")
  func failureWhilePaused() async throws {
    let first = Probe<Int>()
    let second = Probe<String>()
    let failing = Probe<Bool>()
    let (outputs, emitted) = AsyncStream<Void>.makeStream()
    let (gate, proceed) = AsyncStream<Void>.makeStream()
    let consumer = Task { () async throws -> Void in
      var iterator = AsyncOperators.combineLatest(
        first.sequence, second.sequence, failing.sequence
      ).makeAsyncIterator()
      _ = try await iterator.next()
      emitted.yield(())
      let value = try await iterator.next()
      #expect(value?.0 == 2)
      emitted.yield(())
      var gateIterator = gate.makeAsyncIterator()
      _ = await gateIterator.next()
      do {
        _ = try await iterator.next()
        Issue.record("Expected the failure instead of a buffered update")
      } catch {
        #expect(error as? FixtureError == .expected)
      }
      let finished = try await iterator.next()
      #expect(finished == nil)
    }
    first.continuation.yield(1)
    second.continuation.yield("a")
    failing.continuation.yield(true)
    var output = outputs.makeAsyncIterator()
    _ = await output.next()
    // Drain notification of each input's initial read.
    await first.waitForRead()
    await second.waitForRead()
    await failing.waitForRead()

    await first.waitForRead()
    await second.waitForRead()
    await failing.waitForRead()
    first.continuation.yield(2)
    _ = await output.next()
    second.continuation.yield("pending")
    failing.continuation.finish(throwing: FixtureError.expected)
    await failing.waitForTermination()

    proceed.yield(())
    try await consumer.value
  }

  @Test("Dropping an iterator cancels upstream work")
  func droppingIterator() async throws {
    let first = Probe<Int>()
    let second = Probe<String>()
    first.continuation.yield(1)
    second.continuation.yield("a")
    // A separate scope guarantees the iterator's lifetime has ended.
    try await consumeOne(first.sequence, second.sequence)
    await first.waitForTermination()
    await second.waitForTermination()
  }
}

private func consumeOne<A, B>(_ a: A, _ b: B) async throws
where
  A: AsyncSequence & Sendable, B: AsyncSequence & Sendable,
  A.Element: Sendable, B.Element: Sendable
{
  var iterator = AsyncOperators.combineLatest(a, b).makeAsyncIterator()
  _ = try await iterator.next()
}

private struct CountingSequence: AsyncSequence, Sendable {
  let reads: Recorder<Int>

  struct AsyncIterator: AsyncIteratorProtocol {
    let reads: Recorder<Int>
    var value = 0

    mutating func next() async -> Int? {
      guard !Task.isCancelled else { return nil }
      reads.append(value)
      defer { value += 1 }
      return value
    }
  }

  func makeAsyncIterator() -> AsyncIterator { AsyncIterator(reads: reads) }
}

private struct Probe<Value: Sendable>: Sendable {
  struct Sequence: AsyncSequence, Sendable {
    let stream: AsyncThrowingStream<Value, any Error>
    let reads: AsyncStream<Void>.Continuation
    let terminated: AsyncStream<Void>.Continuation

    final class Lifetime {
      let terminated: AsyncStream<Void>.Continuation

      init(_ terminated: AsyncStream<Void>.Continuation) {
        self.terminated = terminated
      }

      deinit { terminated.yield(()) }
    }

    struct AsyncIterator: AsyncIteratorProtocol {
      var iterator: AsyncThrowingStream<Value, any Error>.Iterator
      let reads: AsyncStream<Void>.Continuation
      let lifetime: Lifetime

      mutating func next() async throws -> Value? {
        reads.yield(())
        return try await iterator.next()
      }
    }

    func makeAsyncIterator() -> AsyncIterator {
      AsyncIterator(
        iterator: stream.makeAsyncIterator(), reads: reads, lifetime: Lifetime(terminated)
      )
    }
  }

  let sequence: Sequence
  let continuation: AsyncThrowingStream<Value, any Error>.Continuation
  private let reads: AsyncStream<Void>
  private let terminations: AsyncStream<Void>

  init() {
    let (stream, continuation) = AsyncThrowingStream<Value, any Error>.makeStream()
    let (reads, read) = AsyncStream<Void>.makeStream()
    let (terminations, terminated) = AsyncStream<Void>.makeStream()
    sequence = Sequence(stream: stream, reads: read, terminated: terminated)
    self.continuation = continuation
    self.reads = reads
    self.terminations = terminations
  }

  func waitForRead() async {
    var iterator = reads.makeAsyncIterator()
    _ = await iterator.next()
  }

  func waitForTermination() async {
    var iterator = terminations.makeAsyncIterator()
    _ = await iterator.next()
  }
}
