import AsyncOperators
import Testing

@Suite("Subject")
struct SubjectTests {
  @Test("Subject emits its initial value and updates")
  func currentValueBehavior() async {
    let subject = Subject(1)
    var iterator = subject.makeStream().makeAsyncIterator()

    #expect(await iterator.next() == 1)

    subject.send(2)
    #expect(await iterator.next() == 2)
    #expect(subject.value == 2)

    subject.value = 3
    #expect(await iterator.next() == 3)
    #expect(subject.value == 3)
  }

  @Test("Subject multicasts to independent consumers")
  func multicasts() async {
    let subject = Subject("initial")
    var first = subject.makeStream().makeAsyncIterator()
    var second = subject.makeStream().makeAsyncIterator()

    #expect(await first.next() == "initial")
    #expect(await second.next() == "initial")

    subject.send("next")
    #expect(await first.next() == "next")
    #expect(await second.next() == "next")
  }

  @Test("Finish ends current and future streams and ignores sends")
  func finishIsPermanent() async {
    let subject = Subject(1)
    var current = subject.makeStream().makeAsyncIterator()
    #expect(await current.next() == 1)

    subject.finish()
    #expect(await current.next() == nil)

    subject.send(2)
    #expect(subject.value == 1)

    var future = subject.makeStream().makeAsyncIterator()
    #expect(await future.next() == nil)
  }
}
