import AsyncOperators
import Testing

@Suite("Subscriptions")
struct SubscriptionsTests {
  @Test("Cancel all cancels inserted tasks")
  func cancelAll() async {
    let observedCancellation = LockedBox(false)
    let task = Task {
      while !Task.isCancelled {
        await Task.yield()
      }
      observedCancellation.set(true)
    }
    let subscriptions = Subscriptions()
    subscriptions.insert(task)

    subscriptions.cancelAll()
    await task.value

    #expect(observedCancellation.value)
  }

  @Test("Plus-equals stores tasks and deinit cancels them")
  func operatorCancelsOnDeinit() async {
    let observedCancellation = LockedBox(false)
    let task = Task {
      while !Task.isCancelled {
        await Task.yield()
      }
      observedCancellation.set(true)
    }

    var subscriptions: Subscriptions? = Subscriptions()
    subscriptions! += task
    subscriptions = nil
    await task.value

    #expect(observedCancellation.value)
  }
}
