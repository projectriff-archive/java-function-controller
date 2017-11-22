
/*
 * Copyright (c) 2017. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.projectriff.kubernetes.client;

import io.fabric8.kubernetes.client.Client;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.projectriff.kubernetes.api.model.DoneableTopic;
import io.projectriff.kubernetes.api.model.DoneableXFunction;
import io.projectriff.kubernetes.api.model.FunctionList;
import io.projectriff.kubernetes.api.model.Topic;
import io.projectriff.kubernetes.api.model.TopicList;
import io.projectriff.kubernetes.api.model.XFunction;

public interface RiffClient extends Client {

	MixedOperation<Topic, TopicList, DoneableTopic, Resource<Topic, DoneableTopic>> topics();

	MixedOperation<XFunction, FunctionList, DoneableXFunction, Resource<XFunction, DoneableXFunction>> functions();

}
