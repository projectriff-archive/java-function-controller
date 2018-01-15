/*
 * Copyright 2018 the original author or authors.
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
	"time"
)

type delayer struct {
	idleTimeout map[string]time.Duration
	memory      map[Subscription]timestampedPartionedOffsets
}

type timestampedPartionedOffsets struct {
	PartitionedOffsets
	timestamp time.Time
}

func (d *delayer) delay(offsets map[Subscription]PartitionedOffsets) map[Subscription]PartitionedOffsets {
	now := time.Now()
	result := make(map[Subscription]PartitionedOffsets, len(offsets))
	for s, o := range offsets {
		result[s] = make(PartitionedOffsets, len(o))
		for part, offset := range o {
			if offset.Lag() == 0 && offset.Activity() == 0 {
				if now.Before(d.memory[s].timestamp.Add( /*d.idleTimeout[s.Group]*/ 15 * time.Second)) {
					result[s][part] = d.memory[s].PartitionedOffsets[part]
					continue
				}
			}
			d.memory[s] = timestampedPartionedOffsets{PartitionedOffsets: o, timestamp: now}
			result[s][part] = offsets[s][part]
		}
	}
	log.Printf("Cache: %v", d.memory)
	return result
}

func NewDelayer() delayer {
	return delayer{memory: make(map[Subscription]timestampedPartionedOffsets)}
}
