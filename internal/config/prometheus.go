package config

type Prometheus struct {
	Listen string `env:"PROMETHEUS_LISTEN" envDefault:":2112"`
}
