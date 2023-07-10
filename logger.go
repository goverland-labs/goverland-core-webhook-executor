package main

import (
	"github.com/rs/zerolog/log"
	"github.com/s-larionov/process-manager"
)

type ProcessManagerLogger struct {
}

func (l *ProcessManagerLogger) Info(msg string, fields ...process.LogFields) {
	log.Info().Fields(convertFields(fields)).Msg(msg)
}

func (l *ProcessManagerLogger) Error(msg string, err error, fields ...process.LogFields) {
	log.Error().Err(err).Fields(convertFields(fields)).Msg(msg)
}

func convertFields(fields []process.LogFields) map[string]interface{} {
	if len(fields) == 0 {
		return nil
	}

	return fields[0]
}
