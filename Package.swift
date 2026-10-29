// swift-tools-version: 6.3

import PackageDescription

let package = Package(
  name: "AsyncOperators",
  platforms: [
    .iOS(.v26),
    .macOS(.v26),
    .tvOS(.v26),
    .watchOS(.v26),
    .visionOS(.v26),
  ],
  products: [
    .library(name: "AsyncOperators", targets: ["AsyncOperators"])
  ],
  dependencies: [
    .package(
      url: "https://github.com/apple/swift-async-algorithms.git",
      branch: "main"
    )
  ],
  targets: [
    .target(
      name: "AsyncOperators",
      dependencies: [
        .product(
          name: "AsyncAlgorithms",
          package: "swift-async-algorithms"
        )
      ]
    ),
    .testTarget(
      name: "AsyncOperatorsTests",
      dependencies: ["AsyncOperators"]
    ),
  ]
)
