package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	statusInProgress = "IN_PROGRESS"
	statusCompleted  = "COMPLETED"

	// Dedupe rows outlive the source queue's 4-day retention, so a late
	// duplicate can never arrive after its dedupe row has expired.
	dedupeTTL = 7 * 24 * time.Hour

	// How long an IN_PROGRESS claim blocks other workers. Matches the function
	// timeout: if a worker crashes mid-event, the claim frees itself after this.
	claimLease = 30 * time.Second
)

// OrderEvent is the JSON body published to SNS (raw delivery, so it arrives
// as the SQS message body unchanged).
type OrderEvent struct {
	EventID  string  `json:"eventId"`
	OrderID  string  `json:"orderId"`
	Amount   float64 `json:"amount"`
	Customer string  `json:"customer,omitempty"`
	// SimulateFailure makes doWork fail, for demoing retries and the DLQ.
	SimulateFailure bool `json:"simulateFailure,omitempty"`
}

type app struct {
	db          *dynamodb.Client
	dedupeTable string
	ordersTable string
	log         *slog.Logger
}

// errInFlight means another worker currently holds the claim on this event.
var errInFlight = errors.New("event is being processed by another worker")

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(fmt.Sprintf("load aws config: %v", err))
	}
	a := &app{
		db:          dynamodb.NewFromConfig(cfg),
		dedupeTable: mustEnv("TABLE_NAME"),
		ordersTable: mustEnv("ORDERS_TABLE"),
		log:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}
	lambda.Start(a.handle)
}

// handle processes a batch and returns only the failed message IDs, so SQS
// retries just those instead of the whole batch (ReportBatchItemFailures).
func (a *app) handle(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, msg := range ev.Records {
		if err := a.processMessage(ctx, msg); err != nil {
			a.log.Error("message failed",
				"messageId", msg.MessageId,
				"receiveCount", msg.Attributes["ApproximateReceiveCount"],
				"error", err.Error())
			resp.BatchItemFailures = append(resp.BatchItemFailures,
				events.SQSBatchItemFailure{ItemIdentifier: msg.MessageId})
		}
	}
	a.log.Info("batch done", "size", len(ev.Records), "failed", len(resp.BatchItemFailures))
	return resp, nil
}

func (a *app) processMessage(ctx context.Context, msg events.SQSMessage) error {
	var ev OrderEvent
	if err := json.Unmarshal([]byte(msg.Body), &ev); err != nil {
		return fmt.Errorf("bad json: %w", err)
	}
	if ev.EventID == "" || ev.OrderID == "" {
		return errors.New("eventId and orderId are required")
	}

	claimed, err := a.claim(ctx, ev.EventID)
	if err != nil {
		return err
	}
	if !claimed {
		a.log.Info("duplicate skipped", "eventId", ev.EventID, "messageId", msg.MessageId)
		return nil
	}

	if err := a.doWork(ctx, ev); err != nil {
		// Release the claim so the retry can run doWork again.
		if relErr := a.release(ctx, ev.EventID); relErr != nil {
			a.log.Error("release claim failed", "eventId", ev.EventID, "error", relErr.Error())
		}
		return fmt.Errorf("doWork: %w", err)
	}

	if err := a.complete(ctx, ev.EventID); err != nil {
		// doWork already succeeded and is idempotent, so a retry is safe.
		return fmt.Errorf("mark completed: %w", err)
	}
	a.log.Info("processed", "eventId", ev.EventID, "orderId", ev.OrderID)
	return nil
}

// claim reserves eventId for this worker. It returns false (no error) when the
// event has already been completed, and errInFlight when another worker holds
// a live claim on it.
func (a *app) claim(ctx context.Context, eventID string) (bool, error) {
	now := time.Now()
	_, err := a.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(a.dedupeTable),
		Item: map[string]types.AttributeValue{
			"eventId":     str(eventID),
			"status":      str(statusInProgress),
			"lockedUntil": num(now.Add(claimLease).Unix()),
			"expiresAt":   num(now.Add(dedupeTTL).Unix()),
		},
		// New event, or a stale claim left behind by a crashed worker.
		ConditionExpression: aws.String(
			"attribute_not_exists(eventId) OR (#s = :inprog AND lockedUntil < :now)",
		),
		ExpressionAttributeNames: map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inprog": str(statusInProgress),
			":now":    num(now.Unix()),
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return true, nil
	}

	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		return false, fmt.Errorf("claim: %w", err)
	}
	if s, ok := ccf.Item["status"].(*types.AttributeValueMemberS); ok && s.Value == statusCompleted {
		return false, nil
	}
	return false, errInFlight
}

func (a *app) complete(ctx context.Context, eventID string) error {
	_, err := a.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(a.dedupeTable),
		Key:                      map[string]types.AttributeValue{"eventId": str(eventID)},
		UpdateExpression:         aws.String("SET #s = :done REMOVE lockedUntil"),
		ExpressionAttributeNames: map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":done": str(statusCompleted),
		},
	})
	return err
}

func (a *app) release(ctx context.Context, eventID string) error {
	_, err := a.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                aws.String(a.dedupeTable),
		Key:                      map[string]types.AttributeValue{"eventId": str(eventID)},
		ConditionExpression:      aws.String("#s = :inprog"),
		ExpressionAttributeNames: map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inprog": str(statusInProgress),
		},
	})
	return err
}

// doWork is the business side effect: record the order. It is a plain
// overwrite keyed by orderId, so running it twice for the same event is safe.
func (a *app) doWork(ctx context.Context, ev OrderEvent) error {
	if ev.SimulateFailure {
		return errors.New("simulated failure")
	}
	_, err := a.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(a.ordersTable),
		Item: map[string]types.AttributeValue{
			"orderId":     str(ev.OrderID),
			"eventId":     str(ev.EventID),
			"amount":      &types.AttributeValueMemberN{Value: strconv.FormatFloat(ev.Amount, 'f', -1, 64)},
			"customer":    str(ev.Customer),
			"processedAt": str(time.Now().UTC().Format(time.RFC3339)),
		},
	})
	return err
}

func str(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }
func num(n int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic("missing env var " + k)
	}
	return v
}
