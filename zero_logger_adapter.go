package ghbase

import (
	"github.com/rs/zerolog"
)

type ZeroLogAdapter struct {
	ZeroLog zerolog.Logger
}

// implement interface domain.Logger with ZeroLog
// Info
func (z *ZeroLogAdapter) Info(fields ...interface{}) {
	z.ZeroLog.Info().Fields(fields).Msg("")
}

// Infof
func (z *ZeroLogAdapter) Infof(msg string, fields ...interface{}) {
	z.ZeroLog.Info().Fields(fields).Msgf(msg, fields)
}

// Error
func (z *ZeroLogAdapter) Error(fields ...interface{}) {
	z.ZeroLog.Error().Fields(fields).Msg("")
}

// Errorf
func (z *ZeroLogAdapter) Errorf(msg string, fields ...interface{}) {
	z.ZeroLog.Error().Fields(fields).Msgf(msg, fields)
}

// Debugf
func (z *ZeroLogAdapter) Debugf(msg string, fields ...interface{}) {
	z.ZeroLog.Debug().Fields(fields).Msgf(msg, fields)
}
