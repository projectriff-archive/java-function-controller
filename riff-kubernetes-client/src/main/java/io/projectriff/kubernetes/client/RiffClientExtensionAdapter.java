
/*
 * Copyright (c) 2017. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.projectriff.kubernetes.client;

import io.fabric8.kubernetes.client.APIGroupExtensionAdapter;
import io.fabric8.kubernetes.client.Client;
import okhttp3.OkHttpClient;

public class RiffClientExtensionAdapter extends APIGroupExtensionAdapter<RiffClient> {

	@Override
	protected String getAPIGroupName() {
		return "extensions.sk8s.io";
	}

	@Override
	protected RiffClient newInstance(Client client) {
		return new DefaultRiffClient(client.adapt(OkHttpClient.class), client.getConfiguration());
	}

	@Override
	public Class<RiffClient> getExtensionType() {
		return RiffClient.class;
	}
}
