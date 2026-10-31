import AsyncOperators
import Observation
import Synchronization
import Testing

private final class ObservableFixture: Observable, Sendable {
  private let observation = ObservationRegistrar()
  private let storage = Mutex(0)

  var count: Int {
    get {
      observation.access(self, keyPath: \.count)
      return storage.withLock { $0 }
    }
    set {
      observation.withMutation(of: self, keyPath: \.count) {
        storage.withLock { $0 = newValue }
      }
    }
  }
}

@Suite("Observable async values")
struct ObservationTests {
  @Test("Values emits the current and changed values")
  func valuesIncludesCurrentValue() async {
    let fixture = ObservableFixture()
    var iterator = fixture.values(of: \.count).makeAsyncIterator()

    let initial = await iterator.next(isolation: nil)
    #expect(initial == 0)

    fixture.count = 1
    let update = await iterator.next(isolation: nil)
    #expect(update == 1)
  }

  @Test("New values skips the current value")
  func newValuesSkipsCurrentValue() async {
    let fixture = ObservableFixture()
    var iterator = fixture.newValues(of: \.count).makeAsyncIterator()

    let mutation = Task {
      await Task.yield()
      fixture.count = 1
    }

    let update = await iterator.next(isolation: nil)
    #expect(update == 1)
    await mutation.value
  }
}
