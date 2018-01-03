/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controller

import (
	"bytes"
	"regexp"

	"log"

	"github.com/bsm/sarama-cluster"
)

// LagTracker is used to compute how many unprocessed messages each function needs to take care of.
type LagTracker interface {
	// Register a given function for monitoring.
	BeginTracking(Subscription) error

	// Unregister a function for monitoring.
	StopTracking(Subscription) error

	// Compute the current lags for all tracked subscriptions
	Compute() map[Subscription]Offsets
}

// Subscription describes a tracked tuple of topic and consumer group.
type Subscription struct {
	Topic string
	Group string
}

// Offsets gives per-partition information about current and end offsets.
type Offsets struct {
	Partition int
	Current   uint64
	End       uint64
	Lag       uint64
}

type tracker struct {
	subscriptions                   map[Subscription]bool
	consumersByGroup                map[string]*cluster.Consumer
	endOffsetTrackingConsumer       *cluster.Consumer
	endOffsetTrackingConsumerConfig *cluster.Config
	brokers                         []string
}

func (t *tracker) BeginTracking(s Subscription) error {
	t.subscriptions[s] = true
	t.endOffsetTrackingConsumerConfig.Group.Topics.Whitelist = buildRegex(t.subscriptions)
	if t.endOffsetTrackingConsumer == nil {
		c, err := cluster.NewConsumer(t.brokers, "foobar", nil, t.endOffsetTrackingConsumerConfig)
		if err != nil {
			return err
		}
		t.endOffsetTrackingConsumer = c
	}
	if t.consumersByGroup[s.Group] == nil {
		c, err := cluster.NewConsumer(t.brokers, s.Group, nil, nil)
		if err != nil {
			return err
		}
		t.consumersByGroup[s.Group] = c
	}
	return nil
}

func buildRegex(subs map[Subscription]bool) *regexp.Regexp {
	topics := make(map[string]bool, len(subs))
	for s, _ := range subs {
		topics[s.Topic] = true
	}
	var buffer bytes.Buffer
	for t, _ := range topics {
		if buffer.Len() > 0 {
			buffer.WriteString("|")
		}
		buffer.WriteString("^")
		buffer.WriteString(regexp.QuoteMeta(t))
		buffer.WriteString("$")
	}
	return regexp.MustCompile(buffer.String())
}

func (t *tracker) StopTracking(s Subscription) error {
	delete(t.subscriptions, s)
	t.endOffsetTrackingConsumerConfig.Group.Topics.Whitelist = buildRegex(t.subscriptions)
	close := true
	for sub, _ := range t.subscriptions {
		if sub.Group == s.Group {
			// At least still one subscription with the same group: keep consumer
			close = false
			break
		}
	}
	var err error = nil
	if close {
		err = t.consumersByGroup[s.Group].Close()
		delete(t.consumersByGroup, s.Group)
	}
	if len(t.subscriptions) == 0 {
		err = t.endOffsetTrackingConsumer.Close()
		t.endOffsetTrackingConsumer = nil
	}
	return err
}

func (t *tracker) Compute() map[Subscription]Offsets {
	result := make(map[Subscription]Offsets, len(t.subscriptions))

	if t.endOffsetTrackingConsumer != nil {
		log.Printf("HWM: %v", t.endOffsetTrackingConsumer.HighWaterMarks())
	}

	for s, _ := range t.subscriptions {
		result[s] = Offsets{Current: 2, End: 3, Lag: 1, Partition: 0}
	}
	return result
}

func NewLagTracker(brokers []string) LagTracker {
	return &tracker{
		subscriptions:    make(map[Subscription]bool),
		consumersByGroup: make(map[string]*cluster.Consumer),
		brokers:          brokers,
		endOffsetTrackingConsumerConfig: cluster.NewConfig(),
	}
}
