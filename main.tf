terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "region" {
  type    = string
  default = "eu-north-1"
}

provider "aws" {
  region = var.region
}

# ---------------------------------------------------------------------------
# Tunables. Visibility timeout is derived so it can't drift out of sync with
# the function timeout (rubric item 1: >= 6 x function timeout + batch window).
# ---------------------------------------------------------------------------
locals {
  function_timeout_s = 30
  batching_window_s  = 5
  visibility_timeout = 6 * local.function_timeout_s + local.batching_window_s # 185

  # Pick this from something concrete (e.g. downstream DB connection limit)
  # and write the reasoning in your README. Minimum allowed value is 2.
  max_concurrency = 5
}

# ---------------------------------------------------------------------------
# Messaging: SNS topic -> SQS queue (with DLQ)
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "orders" {
  name = "orders-events"
}

resource "aws_sqs_queue" "dlq" {
  name                      = "orders-dlq"
  message_retention_seconds = 1209600 # 14 days, longer than the source queue
}

resource "aws_sqs_queue" "main" {
  name                       = "orders"
  visibility_timeout_seconds = local.visibility_timeout
  message_retention_seconds  = 345600 # 4 days

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

# Lets the SNS topic (and only that topic) send into the queue.
resource "aws_sqs_queue_policy" "allow_sns" {
  queue_url = aws_sqs_queue.main.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.main.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_sns_topic.orders.arn }
      }
    }]
  })
}

resource "aws_sns_topic_subscription" "orders_to_queue" {
  topic_arn            = aws_sns_topic.orders.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.main.arn
  raw_message_delivery = true # message body is your event JSON, not an SNS envelope

  depends_on = [aws_sqs_queue_policy.allow_sns]
}

# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

# Idempotency / inbox table. One row per event ID.
# Your code should set expiresAt (epoch seconds) comfortably longer than the
# source queue's retention (4 days), e.g. 7 days, so a late duplicate can't
# slip past an already-expired dedupe row.
resource "aws_dynamodb_table" "dedupe" {
  name         = "dedupe"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "eventId"

  attribute {
    name = "eventId"
    type = "S"
  }

  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }
}

# Business side effect table (what doWork writes to).
resource "aws_dynamodb_table" "orders_processed" {
  name         = "orders-processed"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "orderId"

  attribute {
    name = "orderId"
    type = "S"
  }
}

# ---------------------------------------------------------------------------
# IAM: Lambda execution role
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name               = "orders-consumer-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Covers sqs:ReceiveMessage / DeleteMessage / GetQueueAttributes + CloudWatch Logs.
resource "aws_iam_role_policy_attachment" "sqs_exec" {
  role       = aws_iam_role.consumer.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
}

data "aws_iam_policy_document" "consumer_dynamo" {
  statement {
    actions = [
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
    ]
    resources = [
      aws_dynamodb_table.dedupe.arn,
      aws_dynamodb_table.orders_processed.arn,
    ]
  }
}

resource "aws_iam_role_policy" "consumer_dynamo" {
  name   = "dynamo-access"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer_dynamo.json
}

# ---------------------------------------------------------------------------
# Lambda
# Build with:
#   GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o bootstrap .
#   mkdir -p build && zip build/function.zip bootstrap
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/orders-consumer"
  retention_in_days = 7
}

resource "aws_lambda_function" "consumer" {
  function_name = "orders-consumer"
  role          = aws_iam_role.consumer.arn
  runtime       = "provided.al2023"
  handler       = "bootstrap"
  architectures = ["arm64"]
  timeout       = local.function_timeout_s

  filename         = "build/function.zip"
  source_code_hash = filebase64sha256("build/function.zip")

  environment {
    variables = {
      TABLE_NAME   = aws_dynamodb_table.dedupe.name
      ORDERS_TABLE = aws_dynamodb_table.orders_processed.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.consumer,
    aws_iam_role_policy_attachment.sqs_exec,
  ]
}

# The managed poller: reads the queue, batches, invokes the function.
resource "aws_lambda_event_source_mapping" "consumer" {
  event_source_arn                   = aws_sqs_queue.main.arn
  function_name                      = aws_lambda_function.consumer.arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = local.batching_window_s
  function_response_types            = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = local.max_concurrency
  }
}

# ---------------------------------------------------------------------------
# Outputs (handy for your demo script)
# ---------------------------------------------------------------------------
output "topic_arn" {
  value = aws_sns_topic.orders.arn
}

output "queue_url" {
  value = aws_sqs_queue.main.id
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}
