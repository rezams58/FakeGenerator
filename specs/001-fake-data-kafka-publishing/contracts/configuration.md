# Contract: Configuration (`appsettings.json` / environment)

Every setting is bound to strongly typed options with `ValidateOnStart`. An invalid value stops the
process before any write or publish happens (FR-022, Constitution IV). Environment overrides use the
standard `Section__Key` form.

## `ConnectionStrings`

| Key | Required | Rule |
|-----|----------|------|
| SqlServer | yes | Non-empty |

## `Kafka`

All instances of a deployment MUST use the same values.

| Key | Default | Rule |
|-----|---------|------|
| BootstrapServers | `localhost:9092` | Non-empty |
| Topic | `orders` | Non-empty; the topic must exist |
| PartitionCount | 6 | ≥ 6. Must not change after events exist (it would remap keys) |
| ClientId | `fake-order-generator` | Non-empty. It is the TransactionalId namespace: `{ClientId}-{Topic}-partition-{id}` |
| Acks | `All` | Must be `All` |
| CompressionType | `Lz4` | Must parse as `CompressionType` |
| LingerMs | 5 | ≥ 0 |
| BatchSize | 262144 | > 0 (producer bytes) |

## `Generator`

| Key | Default | Rule |
|-----|---------|------|
| Enabled | true | |
| TargetEventCount | 500000 | > 0. This is the total of events in the Outbox (Q5) |
| RecordCount | — | **Obsolete** alias of TargetEventCount; logs a warning |
| BatchSize | 5000 | > 0 (events per SQL transaction) |
| IntervalMilliseconds | 100 | ≥ 0 |
| MaxActiveEntities | 10000 | > 0 |
| AdvanceProbability | 0.6 | 0 < p < 1 |
| RandomSeed | null | Optional int, for reproducible runs |
| MinOrderAmount / MaxOrderAmount | 100 / 5000000 | 0 < Min ≤ Max |
| CustomerCount | 100000 | > 0 |
| OrderHistoryDays | 730 | ≥ 0 |
| `Entities:Order:PartitionKey` | `CustomerId` | Must name a field the Order definition exposes |
| `Entities:Order:Lifecycle` | see below | Must pass the graph validation in data-model.md §4 |

`Entities:Order:Lifecycle` default:

```json
{
  "InitialState": "Created",
  "States": ["Created", "Paid", "Shipped", "Delivered", "Cancelled"],
  "TerminalStates": ["Delivered", "Cancelled"],
  "Transitions": [
    { "From": "Created", "To": "Paid",      "Weight": 85 },
    { "From": "Created", "To": "Cancelled", "Weight": 15 },
    { "From": "Paid",    "To": "Shipped",   "Weight": 90 },
    { "From": "Paid",    "To": "Cancelled", "Weight": 10 },
    { "From": "Shipped", "To": "Delivered", "Weight": 100 }
  ]
}
```

## `Publisher`

| Key | Default | Rule |
|-----|---------|------|
| Enabled | true | |
| BatchSize | 500 | > 0 |
| IdleDelayMilliseconds | 100 | ≥ 0 |
| RetryDelayMilliseconds | 1000 | ≥ 0 |
| MaxRetryCount | 10 | ≥ 1 |
| LeaseDurationSeconds | 90 | > LeaseRenewIntervalSeconds |
| LeaseRenewIntervalSeconds | 20 | > 0 |
| MaxPartitionsPerInstance | 1 | ≥ 1. It is an upper bound only; the leases decide the real assignment |

## `Reporting`

| Key | Default | Rule |
|-----|---------|------|
| Directory | `reports` | Writable |
| Environment | machine name | Names the baseline file: `baseline.{Environment}.json` |
| RegressionThresholdPercent | 20 | > 0 |

## Command line

| Argument | Effect |
|----------|--------|
| (none) | Runs the generator and publisher workers, each according to its `Enabled` flag |
| `--verify` | Runs the stream verifier only (read_committed). Writes the run report. The exit code is non-zero on any violation or regression |
