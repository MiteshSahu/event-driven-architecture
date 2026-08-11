# Invalid-file latch reproduction

## What this lab models

The lab assumes that a valid input filename begins with a UUID, for example:

`550e8400-e29b-41d4-a716-446655440000-orders.csv`

That naming rule is an assumption based on the remembered production behavior, not a claim about the original organization's exact implementation.

The intentionally unsafe coordinator creates a `CountDownLatch` whose count equals the number of submitted files. A worker processing an invalid filename returns early without calling `countDown()`. Consequently, the coordinator can never observe all files as complete.

## Reproduce

```bash
./platform reset
./platform up
./platform test-invalid-batch
sleep 12
./platform batch-progress
```

The fixture batch contains two UUID-prefixed CSV files and `invalid-orders.csv`.

## Observed result

On 2026-08-11, the lab reported:

```text
status=STUCK
submittedFiles=3
successfulFiles=2
invalidFiles=1
completionSignals=2
remainingLatchCount=1
parsedRows=2
```

At the same time, `/actuator/health` returned `UP`. A JVM thread dump showed `unsafe-batch-coordinator` in `WAITING`, parked inside `CountDownLatch.await()` at `UnsafeFileBatchService.coordinate`.

This demonstrates the central failure: pod health and process liveness do not prove that a batch is making progress.

## Why it happens

This is not a problem with a signal arriving too early. `CountDownLatch` remembers calls to `countDown()`. The bug is that one terminal path never signals at all:

```text
3 submitted files -> latch count 3
2 valid files     -> two countDown calls -> count 1
1 invalid file    -> early return        -> count remains 1 forever
```

## Intended safe design (next step)

Every accepted file must reach an explicit terminal state such as `SUCCESS`, `INVALID`, or `FAILED`, and completion must be signalled from a `finally` block or represented with structured futures. The coordinator also needs a timeout and progress-aware health/metrics; a plain liveness probe cannot detect this failure.

## Fixed-path comparison

The safe implementation applies two protections:

1. Each worker increments the completion signal and calls `countDown()` in a `finally` block.
2. The coordinator uses a five-second timed wait instead of waiting forever.

Run both paths with the same fixture batch:

```bash
./platform test-invalid-batch
./platform test-safe-batch
sleep 11
./platform batch-progress
./platform safe-batch-progress
```

Measured comparison on 2026-08-11:

| Measurement | Unsafe path | Safe path |
| --- | ---: | ---: |
| Submitted files | 3 | 3 |
| Successful files | 2 | 2 |
| Invalid files | 1 | 1 |
| Completion signals | 2 | 3 |
| Remaining latch count | 1 | 0 |
| Final status | `STUCK` | `COMPLETED_WITH_REJECTIONS` |

The fix does not pretend that the invalid file succeeded. It records the file as rejected while still allowing the batch itself to reach a terminal state.
