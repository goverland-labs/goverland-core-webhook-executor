package config

import (
	"fmt"
	"time"
)

type Nats struct {
	URL              string        `env:"NATS_URL" envDefault:"nats://127.0.0.1:4222"`
	MaxReconnects    int           `env:"NATS_MAX_RECONNECTS" envDefault:"10"`
	ReconnectTimeout time.Duration `env:"NATS_RECONNECT_TIMEOUT" envDefault:"1s"`
}

func GenerateGroupName(subgroup string) string {
	return fmt.Sprintf("datasource_snapshot_%s", subgroup)
}
