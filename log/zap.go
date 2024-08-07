package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger
var ZapLog *zap.Logger

func InitLogger(level string, paths []string) {
	var err error
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	zaplevel := zapcore.InfoLevel
	zaplevel.UnmarshalText([]byte(level))
	cfg.Level.SetLevel(zaplevel)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	if len(paths) == 0 {
		paths = []string{"stdout"}
	}
	cfg.OutputPaths = paths
	ZapLog, err = cfg.Build()
	if err != nil {
		panic(err)
	}
	Logger = ZapLog.Sugar()
}
