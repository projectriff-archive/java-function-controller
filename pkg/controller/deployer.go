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
	"github.com/projectriff/kubernetes-crds/pkg/apis/projectriff.io/v1"
	kapiV1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Deployer allows the realisation of a function on k8s and its subsequent scaling to accommodate more/less load.
type Deployer interface {
	// Deploy requests that a function be initially deployed on k8s.
	Deploy(function *v1.Function)

	// Undeploy is called when a function is unregistered.
	Undeploy(function *v1.Function)

	// Scale is used to vary the number of replicas dedicated to a function, including going to zero.
	Scale(function *v1.Function, replicas int)
}

type deployer struct {
	clientset *kubernetes.Clientset
}

func (d *deployer) Deploy(function *v1.Function) error {
	deployment := v1beta1.Deployment{kapiV1.ObjectMeta{Name: function.Name, Namespace: function.Namespace}}
	_, err := d.clientset.ExtensionsV1beta1().Deployments(function.Namespace).Create()
	if err != nil {
		return err
	}
}

func (d *deployer) Undeploy(function *v1.Function) {

}

func (d *deployer) Scale(function *v1.Function, replicas int) {

}

func NewDeployer(config *rest.Config) (Deployer, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &deployer{clientset: clientset}, nil
}
