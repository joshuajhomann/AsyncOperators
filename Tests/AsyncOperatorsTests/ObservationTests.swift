import AsyncOperators
import Observation
import Testing

@Observable
@MainActor
private final class ObservableFixture {
  var count = 0
}

@Suite("Observable async values")
@MainActor
struct ObservationTests {
  @Test("Values emits the current and changed values")
  func valuesIncludesCurrentValue() async {
    let fixture = ObservableFixture()
    var iterator = fixture.values(of: \.count).makeAsyncIterator()

    let initial = await iterator.next(isolation: MainActor.shared)
    #expect(initial == 0)

    fixture.count = 1
    let update = await iterator.next(isolation: MainActor.shared)
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

    let update = await iterator.next(isolation: MainActor.shared)
    #expect(update == 1)
    await mutation.value
  }
}
