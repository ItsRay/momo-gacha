package logger

import (
	"log"
)

// Info logs messages with INFO prefix.
func Info(format string, v ...interface{}) {
	log.Printf("[INFO] "+format, v...)
}

// Error logs messages with ERROR prefix.
func Error(format string, v ...interface{}) {
	log.Printf("[ERROR] "+format, v...)
}

// Warn logs messages with WARN prefix.
func Warn(format string, v ...interface{}) {
	log.Printf("[WARN] "+format, v...)
}
