package config

type Health struct {
	Listen string `env:"HEALTH_LISTEN" envDefault:":3000"`
}
