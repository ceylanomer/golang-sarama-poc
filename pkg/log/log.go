// Package log provides logging configuration using uber-go/zap
package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// init initializes the global logger with production settings
// This function automatically runs when the package is imported
func init() {
	// Configure the JSON encoder with custom settings
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"                     // Use "timestamp" as the key for time field
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder   // Use ISO8601 format for timestamps
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder // Use capital letters for log levels (INFO, ERROR, etc.)

	// Create the logger core with JSON encoding and stdout output
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		zapcore.InfoLevel, // Set minimum logging level to INFO
	)

	// Create the logger with caller information (file and line number)
	logger := zap.New(core, zap.AddCaller())

	// Set this logger as the global logger for the application
	zap.ReplaceGlobals(logger)
}
