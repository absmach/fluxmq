// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authatom

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

var (
	errUnsupportedTopic       = errors.New("unsupported topic")
	errInvalidTopicRoute      = errors.New("invalid topic route")
	errUnsupportedAliasRoute  = errors.New("unsupported alias route")
	errUnsupportedTopicFilter = errors.New("unsupported topic filter")
)

type topicAction string

const (
	actionPublish   topicAction = "publish"
	actionSubscribe topicAction = "subscribe"
)

type topicRoute struct {
	tenant       string
	channel      string
	subpath      string
	tenantUUID   bool
	channelUUID  bool
	normalized   string
	raw          string
	isSubscribe  bool
	channelAlias string
	tenantAlias  string
}

func parseMagistralaTopic(raw string, action topicAction) (topicRoute, error) {
	normalized := strings.Trim(strings.TrimSpace(raw), "/")
	if normalized == "" {
		return topicRoute{}, errUnsupportedTopic
	}
	if action == actionPublish && strings.ContainsAny(normalized, "+#") {
		return topicRoute{}, errInvalidTopicRoute
	}

	parts := strings.Split(normalized, "/")
	if len(parts) < 4 || parts[0] != "m" || parts[2] != "c" {
		return topicRoute{}, errUnsupportedTopic
	}
	if parts[1] == "" || parts[3] == "" {
		return topicRoute{}, errInvalidTopicRoute
	}
	if isWildcardLevel(parts[1]) || isWildcardLevel(parts[3]) {
		return topicRoute{}, errUnsupportedTopicFilter
	}
	if action == actionSubscribe {
		if err := validateSubscribeTail(parts[4:]); err != nil {
			return topicRoute{}, err
		}
	}

	tenantUUID := isUUID(parts[1])
	channelUUID := isUUID(parts[3])
	if !tenantUUID && channelUUID {
		return topicRoute{}, errUnsupportedAliasRoute
	}

	route := topicRoute{
		tenant:      parts[1],
		channel:     parts[3],
		tenantUUID:  tenantUUID,
		channelUUID: channelUUID,
		normalized:  normalized,
		raw:         raw,
		isSubscribe: action == actionSubscribe,
	}
	if len(parts) > 4 {
		route.subpath = strings.Join(parts[4:], "/")
	}
	if !route.tenantUUID {
		route.tenantAlias = normalizeAlias(route.tenant)
	}
	if !route.channelUUID {
		route.channelAlias = normalizeAlias(route.channel)
	}

	return route, nil
}

func validateSubscribeTail(parts []string) error {
	for i, part := range parts {
		switch {
		case part == "":
			continue
		case part == "#":
			if i != len(parts)-1 {
				return errUnsupportedTopicFilter
			}
		case strings.Contains(part, "#"):
			return errUnsupportedTopicFilter
		case part == "+":
			continue
		case strings.Contains(part, "+"):
			return errUnsupportedTopicFilter
		}
	}
	return nil
}

func isWildcardLevel(level string) bool {
	return level == "+" || level == "#" || strings.ContainsAny(level, "+#")
}

func isUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}

func normalizeAlias(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
