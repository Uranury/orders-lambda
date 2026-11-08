# Practice 5: Event-driven order processing (SNS → SQS → Lambda → DynamoDB)

An order event is published once to an SNS topic. It is delivered to an SQS
queue, and a Go Lambda consumes it from there in batches. The Lambda records the
order in DynamoDB exactly once, even when the same event arrives more than once.
Messages that keep failing end up in a dead-letter queue (DLQ).

Everything is defined in `main.tf` (Terraform, region `eu-north-1`).

## Architecture

```
  publisher (aws sns publish / any producer)
        │
        ▼
┌──────────────────────┐
│ SNS topic            │  orders-events
└──────────┬───────────┘
           │ subscription (protocol sqs, raw_message_delivery = true)
           │ allowed by: SQS queue policy (only this topic may SendMessage)
           ▼
┌──────────────────────┐   after 5 failed receives   ┌──────────────────────┐
│ SQS queue  "orders"  │ ──── redrive_policy ──────► │ SQS DLQ "orders-dlq" │
│ visibility 185 s     │                             │ retention 14 days    │
│ retention 4 days     │                             └──────────────────────┘
└──────────┬───────────┘
           │ polled by Lambda event source mapping
           │ (batch 10, window 5 s, max concurrency 5,
           │  ReportBatchItemFailures)
           ▼
┌──────────────────────┐   assumes   ┌──────────────────────────────┐
│ Lambda               │ ◄────────── │ IAM role orders-consumer-role│
│ orders-consumer (Go) │             └──────────────────────────────┘
│ arm64, al2023, 30 s  │
└───┬──────────────┬───┘
    │              │ logs
    │              ▼
    │      CloudWatch Logs /aws/lambda/orders-consumer (7 days)
    │
    ├──► DynamoDB "dedupe"            (eventId → claim / COMPLETED, TTL 7 days)
    └──► DynamoDB "orders-processed"  (orderId → order record)
```

## Resources

| Resource | Name | Purpose |
|---|---|---|
| `aws_sns_topic` | `orders-events` | Entry point. Producers publish order events here. |
| `aws_sqs_queue` | `orders` | Buffers events for the Lambda. Visibility timeout 185 s, retention 4 days. |
| `aws_sqs_queue` | `orders-dlq` | Holds messages that failed 5 times. Retention 14 days. |
| `aws_sqs_queue_policy` | on `orders` | Lets SNS send into the queue. |
| `aws_sns_topic_subscription` | topic → queue | Delivers every published message to the queue. |
| `aws_dynamodb_table` | `dedupe` | Idempotency table, one row per `eventId`. |
| `aws_dynamodb_table` | `orders-processed` | The business result, one row per `orderId`. |
| `aws_iam_role` | `orders-consumer-role` | The identity the Lambda runs as. |
| `aws_cloudwatch_log_group` | `/aws/lambda/orders-consumer` | Function logs, kept for 7 days. |
| `aws_lambda_function` | `orders-consumer` | The consumer (`main.go`). |
| `aws_lambda_event_source_mapping` | queue → function | The managed poller that reads SQS and invokes the function. |

## Flow, step by step

1. **Publish.** A producer publishes JSON to `orders-events`:
   ```json
   {"eventId":"evt-1","orderId":"ord-1","amount":42.5,"customer":"alice"}
   ```
2. **SNS → SQS.** The subscription delivers the message to the `orders` queue.
   Because `raw_message_delivery = true`, the SQS message body is exactly the
   JSON above. Without raw delivery it would be wrapped in an SNS envelope.
3. **SQS → Lambda.** The event source mapping long-polls the queue. It waits up
   to 5 s to fill a batch of up to 10 messages, then invokes the function. At
   most 5 function instances run at once for this queue (`maximum_concurrency`).
   While a batch is being processed, its messages are invisible to other
   consumers for 185 s.
4. **Claim the event (dedupe).** For each message the function does a
   conditional `PutItem` into `dedupe`:
   `eventId`, `status = IN_PROGRESS`, `lockedUntil = now + 30 s`,
   `expiresAt = now + 7 days`. The write only succeeds if the event is new, or
   if an earlier claim is stale (its worker crashed and `lockedUntil` has
   passed).
   - Claim fails and the row says `COMPLETED`: this is a duplicate. It is
     skipped and counted as a success.
   - Claim fails and the row is still `IN_PROGRESS`: another worker is
     processing it right now. The message is reported as failed so SQS retries
     it later.
5. **Do the work.** `doWork` writes the order into `orders-processed` (keyed by
   `orderId`). The write is a plain overwrite, so repeating it is harmless.
6. **Mark it done.** The dedupe row is updated to `status = COMPLETED`.
7. **If the work fails**, the claim is deleted so a retry can run it again, and
   the message is reported as failed.
8. **Partial batch response.** The function returns only the IDs of the failed
   messages (`batchItemFailures`). SQS deletes the successful ones and makes
   only the failed ones visible again after the visibility timeout.
9. **Dead-letter.** Each delivery increments a message's receive count. After
   `maxReceiveCount = 5` failed receives, SQS moves the message to
   `orders-dlq`, where it stays for up to 14 days for inspection and redrive.

## IAM and policies

There are three separate permission mechanisms.

**1. SQS queue policy (resource-based): who may write to the queue.**
It allows `sqs:SendMessage` for the principal `sns.amazonaws.com`, and only when
`aws:SourceArn` equals the `orders-events` topic ARN. No other topic or account
can inject messages into the queue.

**2. Role trust policy: who may become the role.**
`orders-consumer-role` trusts only `lambda.amazonaws.com` to call
`sts:AssumeRole`.

**3. Role permission policies: what the Lambda may do.**
- `AWSLambdaSQSQueueExecutionRole` (AWS managed): `sqs:ReceiveMessage`,
  `sqs:DeleteMessage` and `sqs:GetQueueAttributes`, used by the event source
  mapping, plus CloudWatch Logs write access.
- `dynamo-access` (inline): `PutItem`, `UpdateItem`, `DeleteItem` and `GetItem`
  on the two tables only. No scans, and no other tables.

### Where credentials come from

- **Terraform on the developer's machine** uses the developer's own AWS login,
  found through the SDK's default credential chain (environment variables, then
  `~/.aws` profiles). The pinned AWS provider (5.x) does not understand
  `aws login` sessions, so we export them as environment variables first:
  `eval "$(aws configure export-credentials --format env)"`.
- **The Lambda never sees the developer's credentials.** When it starts, the
  Lambda service assumes `orders-consumer-role` and injects temporary
  credentials as environment variables (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`). `config.LoadDefaultConfig` in
  `main.go` picks them up, and Lambda rotates them automatically. The function
  can only do what the role allows.

## Tuning choices

| Setting | Value | Why |
|---|---|---|
| Function timeout | 30 s | Each message is a few small DynamoDB calls, so 30 s leaves plenty of headroom. |
| Visibility timeout | 185 s = 6 × 30 + 5 | AWS guidance is at least 6× the function timeout plus the batching window. This means a message can't reappear while a slow or throttled invocation is still working on it. Derived in `locals` so it can't drift. |
| Batch size / window | 10 / 5 s | Fewer invocations under load, with at most 5 s of added latency. |
| `maxReceiveCount` | 5 | Allows for transient errors (throttling, timeouts) while still sending real poison messages to the DLQ quickly. |
| DLQ retention | 14 days | On standard queues a message keeps its original enqueue time when it moves to the DLQ, so DLQ retention must be longer than the source queue's 4 days. |
| Dedupe TTL | 7 days | Longer than the queue's 4-day retention, so a late duplicate can never arrive after its dedupe row has expired. |
| Claim lease | 30 s | Same as the function timeout. A crashed worker's claim frees itself after that. |
| Max concurrency | 5 | Arbitrary, see below. |

**About `maximum_concurrency = 5`:** this value is arbitrary. The only traffic
in this exercise is a few manually published test events, so there is no real
load or downstream limit to size it against. Any value from 2 (the minimum) up
gives the same behaviour here.

In a real system, the value would come from the bottleneck behind the consumer.
For example, if `doWork` wrote to a relational database with a 20-connection
pool shared with other services, you would cap the consumer at the number of
connections it may use (one per instance). Messages beyond that capacity wait
in SQS instead of overloading the database.

## Event format

| Field | Required | Meaning |
|---|---|---|
| `eventId` | yes | Unique ID of the event, used for deduplication. |
| `orderId` | yes | Key in `orders-processed`. |
| `amount` | no | Order amount. |
| `customer` | no | Customer name. |
| `simulateFailure` | no | If `true`, `doWork` fails. Used to demo retries and the DLQ. |

Malformed JSON, or a message without `eventId` or `orderId`, fails every attempt
and therefore ends up in the DLQ.

## Build, deploy, test

```bash
./build.sh                                            # builds build/function.zip
eval "$(aws configure export-credentials --format env)"
terraform init
terraform apply

T=$(terraform output -raw topic_arn)
aws sns publish --topic-arn "$T" --message '{"eventId":"evt-1","orderId":"ord-1","amount":42.5}'
aws sns publish --topic-arn "$T" --message '{"eventId":"evt-1","orderId":"ord-1","amount":42.5}'   # duplicate
aws sns publish --topic-arn "$T" --message '{"eventId":"evt-bad","orderId":"ord-bad","simulateFailure":true}'

aws dynamodb scan --table-name orders-processed
aws dynamodb scan --table-name dedupe
aws logs tail /aws/lambda/orders-consumer --since 15m
```

Results observed:

- `evt-1` was processed once: one row in `orders-processed`, and its dedupe row
  is `COMPLETED`.
- The second `evt-1` was logged as `duplicate skipped`, with no second write.
- `evt-bad` failed on each attempt, and only that message was retried. It
  reappeared roughly every 185 s, and after 5 receives it moves to `orders-dlq`.

To replay messages from the DLQ after fixing a bug:

```bash
aws sqs start-message-move-task --source-arn <orders-dlq ARN>
```

Replays are safe because events that already completed are skipped by the
dedupe table.

Tear down with `terraform destroy`.
