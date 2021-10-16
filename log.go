package azalea

import (
	"fmt"
	"os"
	"time"
)

// Component gives each aspect of azalea an id they can use for identification
type Component int

const (
	ConsensusModuleId Component = iota
)

// Level are the string literal logging levels available
type Level string

const (
	Debug Level = "Debug"
	Info        = "Info"
	Error       = "Error"
)

type logger struct {
	component     Component
	componentName string
	debug         bool
}

func (l logger) determineName() {
	switch l.component {
	case ConsensusModuleId:
		l.componentName = "consensus_module"
	}
}

func (l logger) shouldDebug() {
	switch l.component {
	case ConsensusModuleId:
		if os.Getenv(ConsensusModuleLogLevel) == "0" {
			l.debug = true
		}
	}
}

func (l logger) init() {
	l.determineName()
	l.shouldDebug()
}

func (l logger) Prefix(level Level) {
	fmt.Printf("[%s] [%s] [%s]", time.Now().String(), l.componentName, level)
}

func (l logger) Log(level Level, format string, args ...interface{}) {
	if level == Debug && !l.debug {
		return
	}

	l.Prefix(level)
	fmt.Printf(format, args...)
	fmt.Printf("\n")
}

var (
	consensusModuleLogger = logger{component: ConsensusModuleId}
)

func Log(id Component, level Level, format string, args ...interface{}) {
	switch id {
	case ConsensusModuleId:
		consensusModuleLogger.Log(level, format, args...)
	}
}

func init() {
	// Init up all the loggers
	consensusModuleLogger.init()
}
