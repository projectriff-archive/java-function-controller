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

// LagTracker is used to compute how many unprocessed messages each function needs to take care of.
type LagTracker interface {
	// Register a given function for monitoring.
	BeginTracking(Subscription)

	// Unregister a function for monitoring.
	StopTracking(Subscription)

	// Compute the current lags for all tracked subscriptions
	Compute() map[Subscription]Offsets
}

// Subscription describes a tracked tuple of topic and consumer group.
type Subscription struct {
	Topic string
	Group string
}

// Offsets gives per-partition information about current and end offsets
type Offsets struct {
	Partition int
	Current   uint64
	End       uint64
	Lag       uint64
}

type tracker struct {
	subscriptions map[Subscription]bool
}

func (t *tracker) BeginTracking(s Subscription) {
	t.subscriptions[s] = true
}

func (t *tracker) StopTracking(s Subscription) {
	delete(t.subscriptions, s)
}

func (t *tracker) Compute() map[Subscription]Offsets {
	result := make(map[Subscription]Offsets, len(t.subscriptions))

	for s, _ := range t.subscriptions {
		result[s] = Offsets{Current: 2, End: 3, Lag: 1, Partition: 0}
	}
	return result
}

func NewLagTracker() LagTracker {
	return &tracker{subscriptions: make(map[Subscription]bool)}
}
