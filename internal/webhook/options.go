package webhook

import (
	"net/http"
)

type ConsumerOpt func(c *Consumer)

func WithHTTPClient(client *http.Client) ConsumerOpt {
	return func(c *Consumer) {
		c.httpClient = client

		if c.httpClient == nil {
			c.httpClient = http.DefaultClient
		}
	}
}
