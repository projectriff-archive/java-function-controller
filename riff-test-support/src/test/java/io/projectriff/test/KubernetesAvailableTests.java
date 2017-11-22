/*
 * Copyright 2017 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package io.projectriff.test;

import org.junit.ClassRule;
import org.junit.Test;

import io.projectriff.test.KafkaAvailableRule;
import io.projectriff.test.KubernetesAvailableRule;
import io.projectriff.test.RiffTypesAvailableRule;

/**
 * @author David Turanski
 **/
public class KubernetesAvailableTests {

	@ClassRule
	public static KubernetesAvailableRule kubernetesAvailable = new KubernetesAvailableRule();

	@ClassRule
	public static KafkaAvailableRule kafkaAvailableRule = new KafkaAvailableRule();

	@ClassRule
	public static RiffTypesAvailableRule riffTypesAvailableRule = new RiffTypesAvailableRule();

	@Test
	public void test() {
	}
}

