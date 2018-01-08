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
	subscriptions map[Subscription]bool
	client        *cluster.Client
}

func (t *tracker) BeginTracking(s Subscription) error {
	t.subscriptions[s] = true
	return nil
}

func (t *tracker) StopTracking(s Subscription) error {
	delete(t.subscriptions, s)
	return nil
}

func (t *tracker) Compute() map[Subscription][]Offsets {
	result := make(map[Subscription][]Offsets, len(t.subscriptions))

	for s, _ := range t.subscriptions {
		result[s] = t.offsetsForSubscription(s)
	}
	return result
}

func (t *tracker) offsetsForSubscription(s Subscription) []Offsets {
	parts, err := t.client.Partitions(s.Topic)
	if err != nil {

	}

	os := make([]Offsets, len(parts))

	offsetManager, err := sarama.NewOffsetManagerFromClient(s.Group, t.client)
	if err != nil {
		log.Printf("Got error %v", err)
	}
	defer offsetManager.Close()

	for index, part := range parts {
		end, err := t.client.GetOffset(s.Topic, part, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Got error %v", err)
		}

		pom, err := offsetManager.ManagePartition(s.Topic, part)
		if err != nil {
			log.Printf("Got error %v", err)
		}
		off, _ := pom.NextOffset()
		var current int64
		if off == sarama.OffsetNewest {
			current = 0
		} else {
			current = off
		}
		os[index] = newOffsets(part, end, current)
		err = pom.Close()
	}
	return os
}

func newOffsets(part int32, end int64, current int64) Offsets {
	return Offsets{Partition: part, End: end, Current: current, Lag: end - current}
}

func NewLagTracker(brokers []string) LagTracker {
	c, _ := cluster.NewClient(brokers, nil)
	return &tracker{
		subscriptions: make(map[Subscription]bool),
		client:        c,
	}
}

func (o Offsets) String() string {
	return fmt.Sprintf("Offsets[p=%v, lag= %v (=%v-%v)]", o.Partition, o.Lag, o.End, o.Current)
}
