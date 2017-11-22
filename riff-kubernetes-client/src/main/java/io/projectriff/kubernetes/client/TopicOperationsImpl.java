
/*
 * Copyright (c) 2017. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.projectriff.kubernetes.client;

import java.util.Map;
import java.util.TreeMap;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.HasMetadataOperation;
import io.projectriff.kubernetes.api.model.DoneableTopic;
import io.projectriff.kubernetes.api.model.Topic;
import io.projectriff.kubernetes.api.model.TopicList;
import okhttp3.OkHttpClient;

public class TopicOperationsImpl
		extends HasMetadataOperation<Topic, TopicList, DoneableTopic, Resource<Topic, DoneableTopic>> {

	public TopicOperationsImpl(OkHttpClient client, Config config, String namespace) {
		this(client, config, "v1", namespace, null, true, null, null, false, -1, new TreeMap<String, String>(),
				new TreeMap<String, String>(), new TreeMap<String, String[]>(), new TreeMap<String, String[]>(),
				new TreeMap<String, String>());
	}

	public TopicOperationsImpl(OkHttpClient client, Config config, String apiVersion, String namespace, String name,
			Boolean cascading, Topic item, String resourceVersion, Boolean reloadingFromServer, long gracePeriodSeconds,
			Map<String, String> labels, Map<String, String> labelsNot, Map<String, String[]> labelsIn,
			Map<String, String[]> labelsNotIn, Map<String, String> fields) {
		super(client, config, "extensions.sk8s.io", apiVersion, "topics", namespace, name, cascading, item,
				resourceVersion, reloadingFromServer, gracePeriodSeconds, labels, labelsNot, labelsIn, labelsNotIn,
				fields);
	}
}
