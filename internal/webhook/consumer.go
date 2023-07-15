package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/goverland-labs/platform-events/events/core"
	client "github.com/goverland-labs/platform-events/pkg/natsclient"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/goverland-labs/core-webhook-executor/internal/config"
)

const (
	groupName = "webhook"

	maxPendingCallbacks = 100
	rateLimit           = 500 * client.KiB
	executionTtl        = time.Minute
)

var (
	ErrInvalidWebhookURL = errors.New("invalid webhook url")
)

type Consumer struct {
	conn     *nats.Conn
	consumer *client.Consumer[core.CallbackPayload]

	httpClient *http.Client
}

func NewConsumer(nc *nats.Conn, opts ...ConsumerOpt) *Consumer {
	c := &Consumer{
		conn:       nc,
		httpClient: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Consumer) Start(ctx context.Context) error {
	group := config.GenerateGroupName(groupName)
	name := fmt.Sprintf("%s/%s", group, core.SubjectCallback)

	// TODO: Might we should move it to the configuration

	opts := []client.ConsumerOpt{
		client.WithMaxAckPending(maxPendingCallbacks),
		client.WithRateLimit(rateLimit),
		client.WithAckWait(executionTtl),
	}

	consumer, err := client.NewConsumer(ctx, c.conn, group, core.SubjectCallback, c.handler(), opts...)
	if err != nil {
		return fmt.Errorf("consume %s: %w", name, err)
	}

	c.consumer = consumer

	// todo: handle correct stopping the consumer by context
	// todo: handle an error on the connection level

	<-ctx.Done()

	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
		log.Error().Err(ctx.Err()).Str("consumer", name).Msg("unexpected context cancelation")
	}

	return c.consumer.Close()
}

func (c *Consumer) handler() core.CallbackHandler {
	return func(payload core.CallbackPayload) error {
		if err := c.validateWebhook(payload); err != nil {
			return err
		}

		return c.execute(payload)
	}
}

func (c *Consumer) execute(wh core.CallbackPayload) error {
	// Prepare the request
	var body io.Reader
	if len(wh.Body) > 0 {
		body = bytes.NewReader(wh.Body)
	}

	req, err := http.NewRequest(http.MethodPost, wh.WebhookURL, body)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	log.Debug().
		Fields(map[string]interface{}{
			"content": string(wh.Body),
			"url":     wh.WebhookURL,
			"status":  resp.StatusCode,
		}).
		Msg("webhook execution")

	// TODO: Retries if the response status is not correct (2xx)

	return nil
}

func (c *Consumer) validateWebhook(wh core.CallbackPayload) error {
	if wh.WebhookURL == "" {
		return ErrInvalidWebhookURL
	}

	if _, err := url.Parse(wh.WebhookURL); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidWebhookURL, err)
	}

	return nil
}
