package server

import (
	"regexp"
)

var topicValid = regexp.MustCompile(`^[a-z0-9_\-.]+$`)

func isValidTopic(topic string) bool {
	return topicValid.MatchString(topic)
}

// really, go?
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
