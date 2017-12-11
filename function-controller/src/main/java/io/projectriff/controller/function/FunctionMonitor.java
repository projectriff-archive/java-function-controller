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

package io.projectriff.controller.function;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;

import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.projectriff.core.resource.ResourceAddedOrModifiedEvent;
import io.projectriff.core.resource.ResourceDeletedEvent;
import io.projectriff.kubernetes.api.model.Topic;
import io.projectriff.kubernetes.api.model.TopicSpec;
import io.projectriff.kubernetes.api.model.XFunction;

/**
 * Listens for function registration/un-registration and periodically scales deployment of
 * function pods based on the evaluation of a SpEL expression.
 *
 * @author Eric Bottard
 * @author Mark Fisher
 */
public class FunctionMonitor {

	private static final int DEFAULT_IDLE_TIMEOUT = 10_000;

	private static final String FUNCTION_REPLICA_TOPIC = "function-replicas";

	private final Logger logger = LoggerFactory.getLogger(FunctionMonitor.class);

	// TODO: Change key to ObjectReference or similar for all these maps
	// Key is function name
	private final Map<String, XFunction> functions = new ConcurrentHashMap<>();

	private final Map<String, Topic> topics = new ConcurrentHashMap<>();

	private final Map<String, CountDownLatch> topicsReady = new ConcurrentHashMap<>();

	/** Keeps track of what the deployments ask. */
	private final Map<String, Integer> actualReplicaCount = new ConcurrentHashMap<>();

	/** Keeps track of how many deployment replicas are marked 'available' */
	private final Map<String, Integer> availableReplicaCount = new ConcurrentHashMap<>();

	private final Map<String, Float> smoothedReplicaCount = new ConcurrentHashMap<>();

	/** Keeps track of the last time this decided to scale down (but not yet effective). */
	private final Map<String, Long> scaleDownStartTimes = new ConcurrentHashMap<>();

	/**
	 * Keeps track of the sum of end positions for all partitions of a scaling-to-0 function's
	 * input topic
	 */
	private final Map<String, Long> scaleDownPositionSums = new ConcurrentHashMap<>();

	@Autowired
	private FunctionDeployer deployer;

	@Autowired
	private EventPublisher publisher;

	private volatile long scalerIntervalMs = 0L; // Wait forever

	private final Thread scalerThread = new Thread(new Scaler());

	private final AtomicBoolean running = new AtomicBoolean();

	private final LagTracker lagTracker = new LagTracker(
			System.getenv("SPRING_CLOUD_STREAM_KAFKA_BINDER_BROKERS"));

	@EventListener
	public synchronized void onFunctionRegistered(ResourceAddedOrModifiedEvent<XFunction> event) {
		XFunction functionResource = event.getResource();
		String functionName = functionResource.getMetadata().getName();
		this.functions.put(functionName, functionResource);
		this.deployer.deploy(functionResource);
		this.lagTracker.beginTracking(functionName, functionResource.getSpec().getInput());
		this.updateScalerInterval();
		if (this.running.compareAndSet(false, true)) {
			this.scalerThread.start();
		}
		this.notify();
		logger.info("function added: " + functionName);
	}

	@EventListener
	public synchronized void onFunctionUnregistered(ResourceDeletedEvent<XFunction> event) {
		XFunction functionResource = event.getResource();
		String functionName = functionResource.getMetadata().getName();
		this.functions.remove(functionName);
		this.actualReplicaCount.remove(functionName);
		this.deployer.undeploy(functionResource);
		this.lagTracker.stopTracking(functionName, functionResource.getSpec().getInput());
		this.updateScalerInterval();
		this.notify();
		logger.info("function deleted: " + functionName);
	}

	@EventListener
	public void onTopicRegistered(ResourceAddedOrModifiedEvent<Topic> event) {
		Topic topic = event.getResource();
		String name = topic.getMetadata().getName();
		this.topics.put(name, topic);
		this.topicsReady.computeIfAbsent(name, i -> new CountDownLatch(1)).countDown();
	}

	@EventListener
	public void onTopicUnregistered(ResourceDeletedEvent<Topic> event) {
		Topic topic = event.getResource();
		String name = topic.getMetadata().getName();
		this.topics.remove(name);
		this.topicsReady.remove(name);
	}

	@EventListener
	public void onDeploymentRegistered(ResourceAddedOrModifiedEvent<Deployment> event) {
		Deployment deployment = event.getResource();
		if (deployment.getMetadata().getLabels().containsKey("function")) {
			String functionName = deployment.getMetadata().getName();
			Integer replicas = deployment.getStatus().getReplicas();
			replicas = (replicas != null) ? replicas : 0;
			Integer previous = this.actualReplicaCount.put(functionName, replicas);
			if (previous != replicas) {
				this.smoothedReplicaCount.put(functionName, (float) replicas);
			}

			Integer availableReplicas = deployment.getStatus().getAvailableReplicas();
			availableReplicas = (availableReplicas != null) ? availableReplicas : 0;
			previous = this.availableReplicaCount.put(functionName, availableReplicas);
			if (previous != availableReplicas) {
				XFunction functionResource = this.functions.get(functionName);
				if (functionResource != null && !FUNCTION_REPLICA_TOPIC.equals(functionResource.getSpec().getInput())) {
					this.publisher.publish(FUNCTION_REPLICA_TOPIC,
							Collections.singletonMap(functionName, availableReplicas));
				}
			}
		}
	}

	@EventListener
	public void onDeploymentUnregistered(ResourceDeletedEvent<Deployment> event) {
		Deployment deployment = event.getResource();
		if (deployment.getMetadata().getLabels().containsKey("function")) {
			String functionName = deployment.getMetadata().getName();
			this.actualReplicaCount.remove(functionName);
			this.smoothedReplicaCount.remove(functionName);
			this.availableReplicaCount.remove(functionName);
		}
	}

	@PreDestroy
	public synchronized void tearDown() {
		this.running.set(false);
		this.notify();
		this.lagTracker.close();
	}

	private void updateScalerInterval() {
		this.scalerIntervalMs = this.functions.isEmpty() ? 0 : 100;
	}

	private class Scaler implements Runnable {

		@Override
		public void run() {
			while (running.get()) {
				synchronized (FunctionMonitor.this) {
					try {
						scaleFunctions();
					}
					catch (RuntimeException e) {
						logger.warn("Caught exception in loop, continuing...", e);
					}
					try {
						FunctionMonitor.this.wait(scalerIntervalMs);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}

		private void scaleFunctions() {
			Map<LagTracker.Subscription, List<LagTracker.Offsets>> offsets = lagTracker.compute();
			logOffsets(offsets);
			Map<String, Long> lags = offsets.entrySet().stream()
					.collect(Collectors.toMap(
							e -> e.getKey().group,
							e -> e.getValue().stream()
									.mapToLong(LagTracker.Offsets::getLag)
									.max().getAsLong()));
			Map<String, Long> positionSums = offsets.entrySet().stream()
					.collect(Collectors.toMap(
							e -> e.getKey().group,
							e -> e.getValue().stream()
									.mapToLong(LagTracker.Offsets::getEnd).sum()));
			functions.values().stream().forEach(
					f -> {
						String name = f.getMetadata().getName();
						Integer idleTimeout = f.getSpec().getIdleTimeoutMs();
						if (idleTimeout == null) {
							idleTimeout = DEFAULT_IDLE_TIMEOUT;
						}

						long currentPositionSum = positionSums.get(name);
						int desired = computeDesiredReplicaCount(lags, f);
						int current = actualReplicaCount.computeIfAbsent(name, k -> 0);

						if (desired != current) {
							float interpolation = smoothedReplicaCount.computeIfAbsent(name, k -> 0F);

							interpolation = interpolate(interpolation, desired, .05f);
							int rounded = Math.round(interpolation);
							smoothedReplicaCount.put(name, interpolation);

							logger.trace(
									"Want {} for {}. Rounded to {} [target = {}]. (Deployment currently set to {})",
									interpolation, name, rounded, desired, current);
							if (current == 0 && desired > 0) {
								// Special case when scaling from 0
								deployer.scale(f, 1);
							}
							else if (rounded != current) {
								// Special case when going back to 0
								if (rounded == 0) {
									Long start = scaleDownStartTimes.get(name);
									long now = System.currentTimeMillis();
									if (start == null) {
										scaleDownStartTimes.put(name, now);
										scaleDownPositionSums.put(name, currentPositionSum);
									}
									else {
										if (now >= start + idleTimeout) {
											scaleDownStartTimes.remove(name);
											deployer.scale(f, rounded);
										}
										else {
											if (currentPositionSum > scaleDownPositionSums.get(name)) {
												// still active, reset the clock
												scaleDownStartTimes.remove(name);
												scaleDownPositionSums.remove(name);
											}
											else {
												logger.debug("Waiting another {}ms to scale back down to 0 for {}",
														start + idleTimeout - now, name);
											}
										}
									}
								}
								else {
									deployer.scale(f, rounded);
									scaleDownStartTimes.remove(name);
								}
							}
							else {
								scaleDownStartTimes.remove(name);
							}
						}
					});
		}

		/**
		 * Compute the desired replica count for a function. This function leverages these 4
		 * values (currently non configurable):
		 * <ul>
		 * <li>minReplicas (>= 0, default 0)</li>
		 * <li>maxReplicas (minReplicas <= maxReplicas <= partitionCount, default
		 * partitionCount)</li>
		 * <li>lagRequiredForOne, the amount of lag required to trigger the first pod to appear,
		 * default 1</li>
		 * <li>lagRequiredForMax, the amount of lag required to trigger all (maxReplicas) pods to
		 * appear. Default 10.</li>
		 * </ul>
		 * This method linearly interpolates based on witnessed lag and clamps the result between
		 * min/maxReplicas.
		 */
		private int computeDesiredReplicaCount(Map<String, Long> lags, XFunction f) {
			String fnName = f.getMetadata().getName();
			long lag = lags.get(fnName);
			String input = f.getSpec().getInput();
			int partitionCount = partitionCount(input);

			// TODO: those 3 numbers part of Function spec?
			int lagRequiredForMax = 10;
			int lagRequiredForOne = 1;
			int minReplicas = 0;
			
			int maxReplicas = f.getSpec().getMaxReplicas() != null ? f.getSpec().getMaxReplicas() : partitionCount;
			maxReplicas = clamp(maxReplicas, minReplicas, partitionCount);

			double slope = ((double) maxReplicas - 1) / (lagRequiredForMax - lagRequiredForOne);
			int computedReplicas;
			if (slope > 0d) {
				// max>1
				computedReplicas = (int) (1 + (lag - lagRequiredForOne) * slope);
			}
			else {
				computedReplicas = lag >= lagRequiredForOne ? 1 : 0;
			}
			return clamp(computedReplicas, minReplicas, maxReplicas);
		}

		private int partitionCount(String input) {
			waitForTopic(input);
			TopicSpec spec = topics.get(input).getSpec();
			if (spec == null || spec.getPartitions() == null) {
				return 1;
			}
			else {
				return spec.getPartitions();
			}
		}

		private void waitForTopic(String input) {
			try {
				topicsReady.computeIfAbsent(input, i -> new CountDownLatch(1)).await();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		private void logOffsets(Map<LagTracker.Subscription, List<LagTracker.Offsets>> offsets) {
			for (Map.Entry<LagTracker.Subscription, List<LagTracker.Offsets>> entry : offsets
					.entrySet()) {
				logger.trace(entry.getKey().toString());
				for (LagTracker.Offsets values : entry.getValue()) {
					logger.trace("\t" + values + " Lag=" + values.getLag());
				}
			}
		}
	}

	/**
	 * Shoot for value {@literal target}, while currently at {@current}. 'greed' represents
	 * how much of the missing piece we're going to grab in one 'tick'.
	 */
	private static float interpolate(float current, int target, float greed) {
		return current + (target - current) * greed;
	}

	private static int clamp(int value, int min, int max) {
		value = Math.min(value, max);
		value = Math.max(value, min);
		return value;
	}
}
