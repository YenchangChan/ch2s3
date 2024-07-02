package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger
var ZapLog *zap.Logger

func InitLogger(level string) {
	cfg := zap.NewDevelopmentConfig()
	cfg.Encoding = "console"
	zaplevel := zapcore.InfoLevel
	zaplevel.UnmarshalText([]byte(level))
	cfg.Level.SetLevel(zaplevel)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	cfg.OutputPaths = []string{"stdout", "ch2s3.log"}
	ZapLog, _ = cfg.Build()
	Logger = ZapLog.Sugar()
}
