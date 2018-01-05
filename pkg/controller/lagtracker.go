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
	"log"

	"fmt"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// LagTracker is used to compute how many unprocessed messages each function needs to take care of.
type LagTracker interface {
	// Register a given function for monitoring.
	BeginTracking(Subscription) error

	// Unregister a function for monitoring.
	StopTracking(Subscription) error

	// Compute the current lags for all tracked subscriptions
	Compute() map[Subscription][]Offsets
}

// Subscription describes a tracked tuple of topic and consumer group.
type Subscription struct {
	Topic string
	Group string
}

// Offsets gives per-partition information about current and end offsets.
type Offsets struct {
	Partition int32
	Current   int64
	End       int64
	Lag       int64
}

type tracker struct {
	subscriptions             map[Subscription]bool
	endOffsetTrackingConsumer *cluster.Consumer
	brokers                   []string
	client                    *cluster.Client
}

func (t *tracker) BeginTracking(s Subscription) error {
	oldTopics := t.topicsSubscribedTo()
	_, topicAlreadySubscribedTo := oldTopics[s.Topic]
	t.subscriptions[s] = true
	if !topicAlreadySubscribedTo {
		if t.endOffsetTrackingConsumer != nil {
			err := t.endOffsetTrackingConsumer.Close()
			if err != nil {
				return err
			}
		}
		topics := append(keys(oldTopics), s.Topic)
		c, err := cluster.NewConsumer(t.brokers, "foobar", topics, nil)
		if err != nil {
			return err
		}
		t.endOffsetTrackingConsumer = c
	}
	return nil
}

func (t *tracker) StopTracking(s Subscription) error {
	delete(t.subscriptions, s)
	newTopics := t.topicsSubscribedTo()
	_, topicStillSubscribedTo := newTopics[s.Topic]

	if !topicStillSubscribedTo {
		err := t.endOffsetTrackingConsumer.Close()
		if err != nil {
			return err
		}
		if len(newTopics) > 0 { // Recreate consumer with updated set of topics
			c, err := cluster.NewConsumer(t.brokers, "foobar", keys(newTopics), nil)
			if err != nil {
				return nil
			}
			t.endOffsetTrackingConsumer = c
		} else {
			t.endOffsetTrackingConsumer = nil
		}
	}
	return nil
}

func (t *tracker) Compute() map[Subscription][]Offsets {
	result := make(map[Subscription][]Offsets, len(t.subscriptions))

	if t.endOffsetTrackingConsumer == nil {
		return result
	}

	for s, _ := range t.subscriptions {
		parts, err := t.client.Partitions(s.Topic)

		var offsetManager sarama.OffsetManager
		if offsetManager, err = sarama.NewOffsetManagerFromClient(s.Group, t.client); err != nil {
			log.Printf("Got error %v", err)
		}
		os := make([]Offsets, len(parts))
		for index, part := range parts {
			var end int64
			if end, err = t.client.GetOffset(s.Topic, part, sarama.OffsetNewest); err != nil {
				log.Printf("Got error %v", err)
			}

			var pom sarama.PartitionOffsetManager
			if pom, err = offsetManager.ManagePartition(s.Topic, part); err != nil {
				log.Printf("Got error %v", err)
			}
			off, _ := pom.NextOffset()
			os[index].Partition = part
			os[index].End = end
			if off == sarama.OffsetNewest {
				os[index].Current = 0
			} else {
				os[index].Current = off
			}
			os[index].Lag = os[index].End - os[index].Current
		}

		result[s] = os
	}
	return result
}

func NewLagTracker(brokers []string) LagTracker {
	c, _ := cluster.NewClient(brokers, nil)
	return &tracker{
		subscriptions: make(map[Subscription]bool),
		brokers:       brokers,
		client:        c,
	}
}

func (t *tracker) topicsSubscribedTo() map[string]bool {
	result := make(map[string]bool, len(t.subscriptions))
	for s, _ := range t.subscriptions {
		result[s.Topic] = true
	}
	return result
}

func keys(m map[string]bool) []string {
	result := make([]string, len(m), 1+len(m))
	i := 0
	for k := range m {
		result[i] = k
	}
	return result
}

func (o Offsets) String() string {
	return fmt.Sprintf("Offsets[p=%v, lag = %v = %v-%v]", o.Partition, o.Lag, o.End, o.Current)
}
