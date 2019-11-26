package server

import (
	"regexp"
)

var topicValid = regexp.MustCompile(`^[a-z0-9_\-.]+$`)

func isValidTopic(topic string) bool {
	return topicValid.MatchString(topic)
}