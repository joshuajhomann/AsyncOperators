# AsyncOperators

`AsyncOperators` is a Swift 6.3 library of small `AsyncSequence` conveniences
and multicast sources. It contains the operators extracted from the Pokémon
compositional-layout project, plus `Pipe` and `Subject` from the Swift
Playgrounds implementation.

The package follows the `main` branch of Apple's
[`swift-async-algorithms`](https://github.com/apple/swift-async-algorithms)
package.

```swift
import AsyncOperators
```

## Operators at a glance

| API | Purpose |
| --- | --- |
| `subscribe(_:)` | Consume a nonthrowing sequence in a cancellable task. |
| `subscribe(onComplete:_:)` | Consume a sequence and report why it stopped. |
| `subscribe(referencing:_:)` | Consume values without retaining an object. |
| `assign(on:to:)` | Assign tuple elements to matching writable key paths. |
| `values(of:)` | Observe an `@Observable` property, including its current value. |
| `newValues(of:)` | Observe changes to an `@Observable` property after its current value. |
| `Subscriptions` | Own and cancel a group of subscription tasks. |
| `Pipe` | Multicast new values without replaying an earlier value. |
| `Subject` | Multicast a current value and all subsequent values. |

## `subscribe(_:)`

Use this overload with a nonthrowing sequence. It starts a task that consumes
the sequence and returns that task so the subscription can be cancelled or
awaited. The task inherits the caller's actor isolation.

```swift
let (events, continuation) = AsyncStream<String>.makeStream()

let subscription = events.subscribe { event in
  print("Received:", event)
}

continuation.yield("Bulbasaur")
continuation.yield("Ivysaur")
continuation.finish()

await subscription.value
```

Cancelling the returned task stops the subscription:

```swift
subscription.cancel()
```

## `subscribe(onComplete:_:)`

Use the completion overload when the sequence can throw or when the caller
needs to distinguish normal completion from cancellation. The completion
closure is called exactly once with an `AsyncSequenceCompletion` value:

- `.finished` when the sequence ends normally
- `.cancelled` when the subscription task is cancelled
- `.error(Failure)` when iteration throws the sequence's typed failure

```swift
let subscription = responses.subscribe(
  onComplete: { completion in
    switch completion {
    case .finished:
      print("All responses received")
    case .cancelled:
      print("Request cancelled")
    case .error(let error):
      print("Request failed:", error)
    }
  },
  { response in
    await cache.store(response)
  }
)
```

The value handler may be asynchronous. Completion is reported after the final
value handler returns.

## `subscribe(referencing:_:)`

This overload keeps only a weak reference to the supplied object. It is useful
for binding a long-lived sequence to an object with a shorter lifetime. The
subscription exits when the object has been released.

```swift
final class PokemonViewModel {
  var latestName = ""
}

let viewModel = PokemonViewModel()

let subscription = names.subscribe(referencing: viewModel) { viewModel, name in
  viewModel.latestName = name
}
```

There is no need to write a weak capture list in the closure: the operator does
not retain `viewModel`.

## `assign(on:to:)`

`assign` maps the elements of an async tuple to writable key paths in the same
order. It supports any tuple arity using Swift parameter packs and weakly holds
the target.

```swift
final class SearchState {
  var resultCount = 0
  var status = "Idle"
}

let state = SearchState()
let (updates, continuation) = AsyncStream<(Int, String)>.makeStream()

let subscription = updates.assign(
  on: state,
  to: \SearchState.resultCount,
  \SearchState.status
)

continuation.yield((151, "Loaded"))
continuation.finish()
await subscription.value

print(state.resultCount)  // 151
print(state.status)       // Loaded
```

Every tuple element must match the value type of its corresponding key path.

## `values(of:)`

`values(of:)` turns a property on an `Observation.Observable` object into an
async sequence. Its first element is the property's current value; later
elements are emitted when Observation detects a change. The sequence finishes
when the observed object is released.

```swift
import Observation

@Observable
@MainActor
final class SearchModel {
  var query = ""
}

@MainActor
func observeQuery(on model: SearchModel) -> Task<Void, Never> {
  model.values(of: \.query).subscribe { query in
    print("Current query:", query)
  }
}
```

The operator inherits the caller's actor isolation, so actor-isolated models
can be observed on their owning actor.

## `newValues(of:)`

`newValues(of:)` has the same lifetime and isolation behavior as `values(of:)`,
but drops the initial value. Use it when only changes made after subscription
matter.

```swift
@MainActor
func saveQueryChanges(from model: SearchModel) -> Task<Void, Never> {
  model.newValues(of: \.query).subscribe { query in
    await preferences.save(query)
  }
}
```

For a model whose query is already `"Pikachu"`, `values(of:)` first emits
`"Pikachu"`; `newValues(of:)` waits for the next change.

## `Subscriptions`

`Subscriptions` owns the cancellation closures for multiple tasks. It cancels
every stored task when the collection is deinitialized, or immediately when
`cancelAll()` is called.

```swift
final class Coordinator {
  private let subscriptions = Subscriptions()

  func start(names: AsyncStream<String>, ids: AsyncStream<Int>) {
    subscriptions += names.subscribe { name in
      print("Name:", name)
    }

    subscriptions += ids.subscribe { id in
      print("ID:", id)
    }
  }

  func stop() {
    subscriptions.cancelAll()
  }
}
```

Tasks can also be added explicitly:

```swift
subscriptions.insert(subscription)
```

## `Pipe`

`Pipe` is a thread-safe multicast source with passthrough behavior. Each call
to `makeStream()` creates an independent subscription. `send(_:)` forwards a
value to consumers that are currently waiting; it does not retain a value for
late or slow consumers.

```swift
let pipe = Pipe<String>()
let events = pipe.makeStream()

let subscription = events.subscribe { event in
  print("Event:", event)
}

pipe.send("appeared")
pipe.send("selected")
pipe.finish()

await subscription.value
```

Calling `finish()` permanently finishes all current streams. Streams created
afterward are already finished, and later calls to `send(_:)` are ignored.

Use a `Pipe` for events where replay would be incorrect, such as button taps or
refresh requests.

## `Subject`

`Subject` is a thread-safe current-value multicast source. Every stream starts
with the subject's current value and then receives updates. Slow consumers keep
only the newest pending value.

```swift
let isLoading = Subject(false)
let values = isLoading.makeStream()

let subscription = values.subscribe { value in
  print("Loading:", value)
}

isLoading.send(true)
isLoading.value = false  // Assigning value is equivalent to send(false).
isLoading.finish()

await subscription.value
```

The current value can be read synchronously:

```swift
print(isLoading.value)
```

As with `Pipe`, finishing is permanent. A send after `finish()` is ignored and
does not change `value`.

Use a `Subject` for state where a new consumer needs an immediate snapshot,
such as loading state, selection, or the latest model value.

## Requirements

- Swift 6.3 or newer
- iOS 26, macOS 26, tvOS 26, watchOS 26, or visionOS 26

The package is a library, so it has no directly runnable executable product.
Run its test suite with:

```sh
swift test
```
