package internal

import (
	"fmt"
	"log"
)

func Debug(a ...interface{}) {
	if DebugEnabled {
		log.Print("debug: ", fmt.Sprint(a...))
	}
}
func Debugf(format string, a ...interface{}) {
	if DebugEnabled {
		log.Print("debug: ", fmt.Sprintf(format, a...))
	}
}
