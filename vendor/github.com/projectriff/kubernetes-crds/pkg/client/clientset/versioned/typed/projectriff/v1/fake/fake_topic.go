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

package fake

import (
	projectriff_io_v1 "github.com/projectriff/kubernetes-crds/pkg/apis/projectriff.io/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeTopics implements TopicInterface
type FakeTopics struct {
	Fake *FakeProjectriffV1
	ns   string
}

var topicsResource = schema.GroupVersionResource{Group: "projectriff.io", Version: "v1", Resource: "topics"}

var topicsKind = schema.GroupVersionKind{Group: "projectriff.io", Version: "v1", Kind: "Topic"}

// Get takes name of the topic, and returns the corresponding topic object, and an error if there is any.
func (c *FakeTopics) Get(name string, options v1.GetOptions) (result *projectriff_io_v1.Topic, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(topicsResource, c.ns, name), &projectriff_io_v1.Topic{})

	if obj == nil {
		return nil, err
	}
	return obj.(*projectriff_io_v1.Topic), err
}

// List takes label and field selectors, and returns the list of Topics that match those selectors.
func (c *FakeTopics) List(opts v1.ListOptions) (result *projectriff_io_v1.TopicList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(topicsResource, topicsKind, c.ns, opts), &projectriff_io_v1.TopicList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &projectriff_io_v1.TopicList{}
	for _, item := range obj.(*projectriff_io_v1.TopicList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested topics.
func (c *FakeTopics) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(topicsResource, c.ns, opts))

}

// Create takes the representation of a topic and creates it.  Returns the server's representation of the topic, and an error, if there is any.
func (c *FakeTopics) Create(topic *projectriff_io_v1.Topic) (result *projectriff_io_v1.Topic, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(topicsResource, c.ns, topic), &projectriff_io_v1.Topic{})

	if obj == nil {
		return nil, err
	}
	return obj.(*projectriff_io_v1.Topic), err
}

// Update takes the representation of a topic and updates it. Returns the server's representation of the topic, and an error, if there is any.
func (c *FakeTopics) Update(topic *projectriff_io_v1.Topic) (result *projectriff_io_v1.Topic, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(topicsResource, c.ns, topic), &projectriff_io_v1.Topic{})

	if obj == nil {
		return nil, err
	}
	return obj.(*projectriff_io_v1.Topic), err
}

// Delete takes name of the topic and deletes it. Returns an error if one occurs.
func (c *FakeTopics) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(topicsResource, c.ns, name), &projectriff_io_v1.Topic{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeTopics) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(topicsResource, c.ns, listOptions)

	_, err := c.Fake.Invokes(action, &projectriff_io_v1.TopicList{})
	return err
}

// Patch applies the patch and returns the patched topic.
func (c *FakeTopics) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *projectriff_io_v1.Topic, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(topicsResource, c.ns, name, data, subresources...), &projectriff_io_v1.Topic{})

	if obj == nil {
		return nil, err
	}
	return obj.(*projectriff_io_v1.Topic), err
}
