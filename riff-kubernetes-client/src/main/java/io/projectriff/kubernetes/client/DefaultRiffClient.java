
/*
 * Copyright (c) 2017. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.projectriff.kubernetes.client;

import java.lang.reflect.Field;
import java.util.Map;

import io.fabric8.kubernetes.client.BaseClient;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;
import io.projectriff.kubernetes.api.model.DoneableTopic;
import io.projectriff.kubernetes.api.model.DoneableXFunction;
import io.projectriff.kubernetes.api.model.FunctionList;
import io.projectriff.kubernetes.api.model.Topic;
import io.projectriff.kubernetes.api.model.TopicList;
import io.projectriff.kubernetes.api.model.XFunction;
import okhttp3.OkHttpClient;

public class DefaultRiffClient extends BaseClient implements RiffClient {

	static {
		hackKubernetesDeserializer();
	}

	private static void hackKubernetesDeserializer() {
		try {
			Field mapField = KubernetesDeserializer.class.getDeclaredField("MAP");
			mapField.setAccessible(true);
			Map<String, Class<?>> map = (Map<String, Class<?>>) mapField.get(null);

			map.put("Topic", Topic.class);
			map.put("Function", XFunction.class);
		}
		catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public DefaultRiffClient(OkHttpClient okHttpClient, Config configuration) {
		super(okHttpClient, configuration);
	}

	@Override
	public MixedOperation<Topic, TopicList, DoneableTopic, Resource<Topic, DoneableTopic>> topics() {
		return new TopicOperationsImpl(httpClient, getConfiguration(), getNamespace());
	}

	@Override
	public MixedOperation<XFunction, FunctionList, DoneableXFunction, Resource<XFunction, DoneableXFunction>> functions() {
		return new FunctionOperationsImpl(httpClient, getConfiguration(), getNamespace());
	}
}
