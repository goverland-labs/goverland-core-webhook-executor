package main

import (
	"github.com/caarlos0/env/v8"
	"github.com/rs/zerolog"
	"github.com/s-larionov/process-manager"

	"github.com/goverland-labs/goverland-core-webhook-executor/internal"
	"github.com/goverland-labs/goverland-core-webhook-executor/internal/config"
)

var cfg config.App

func init() {
	err := env.Parse(&cfg)
	if err != nil {
		panic(err)
	}

	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		panic(err)
	}
	zerolog.SetGlobalLevel(level)
	process.SetLogger(&ProcessManagerLogger{})
}

func main() {
	app, err := internal.NewApplication(cfg)
	if err != nil {
		panic(err)
	}

	app.Run()
}
