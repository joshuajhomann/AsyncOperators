import AsyncOperators
import Testing

@Suite("Pipe")
struct PipeTests {
  @Test("Pipe multicasts values to current consumers")
  func multicasts() async {
    let pipe = Pipe<Int>()
    let firstStream = pipe.makeStream()
    let secondStream = pipe.makeStream()

    let first = Task {
      var iterator = firstStream.makeAsyncIterator()
      return await iterator.next()
    }
    let second = Task {
      var iterator = secondStream.makeAsyncIterator()
      return await iterator.next()
    }
    let producer = Task {
      for _ in 0..<100 {
        pipe.send(42)
        await Task.yield()
      }
      pipe.finish()
    }

    #expect(await first.value == 42)
    #expect(await second.value == 42)
    await producer.value
  }

  @Test("Pipe does not replay values sent before subscription")
  func doesNotReplay() async {
    let pipe = Pipe<Int>()
    pipe.send(1)

    var iterator = pipe.makeStream().makeAsyncIterator()
    let consumer = Task { await iterator.next() }
    let producer = Task {
      for _ in 0..<100 {
        pipe.send(2)
        await Task.yield()
      }
      pipe.finish()
    }

    #expect(await consumer.value == 2)
    await producer.value
  }

  @Test("Finish ends current and future streams")
  func finishIsPermanent() async {
    let pipe = Pipe<Int>()
    var current = pipe.makeStream().makeAsyncIterator()

    pipe.finish()
    #expect(await current.next() == nil)

    pipe.send(1)
    var future = pipe.makeStream().makeAsyncIterator()
    #expect(await future.next() == nil)
  }
}
