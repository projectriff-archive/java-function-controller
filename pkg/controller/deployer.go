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
	"log"

	"os"

	"strings"

	"github.com/projectriff/kubernetes-crds/pkg/apis/projectriff.io/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	sidecarImage = "projectriff/function-sidecar"
)

var (
	zero = int32(0)
)

// Deployer allows the realisation of a function on k8s and its subsequent scaling to accommodate more/less load.
type Deployer interface {
	// Deploy requests that a function be initially deployed on k8s.
	Deploy(function *v1.Function) error

	// Undeploy is called when a function is unregistered.
	Undeploy(function *v1.Function) error

	// Scale is used to vary the number of replicas dedicated to a function, including going to zero.
	Scale(function *v1.Function, replicas int) error
}

type deployer struct {
	clientset *kubernetes.Clientset
	brokers   []string
}

func (d *deployer) Deploy(function *v1.Function) error {

	deployment := v1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: function.Name, Namespace: function.Namespace},
		Spec: v1beta1.DeploymentSpec{
			Replicas: &zero,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Name: function.Name, Labels: map[string]string{"function": function.Name}},
				Spec:       d.buildPodSpec(function),
			},
		},
	}
	_, err := d.clientset.ExtensionsV1beta1().Deployments(function.Namespace).Create(&deployment)
	if err != nil {
		return err
	}
	return nil
}

func (d *deployer) buildPodSpec(function *v1.Function) corev1.PodSpec {
	spec := corev1.PodSpec{
		Containers: []corev1.Container{d.buildMainContainer(function), d.buildSidecarContainer(function)},
	}
	if needsVolume(function) {
		spec.Volumes = []corev1.Volume{corev1.Volume{Name: "pipes", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
	}
	return spec
}

func (d *deployer) buildMainContainer(function *v1.Function) corev1.Container {
	c := function.Spec.Container
	c.Name = "main"
	if needsVolume(function) {
		c.VolumeMounts = []corev1.VolumeMount{corev1.VolumeMount{Name: "pipes", MountPath: "/pipes"}}
	}
	return c
}

func (d *deployer) buildSidecarContainer(function *v1.Function) corev1.Container {
	c := corev1.Container{Name: "sidecar"}
	c.Image = sidecarImage + ":" + os.Getenv("RIFF_FUNCTION_SIDECAR_TAG")
	outputDestination := function.Spec.Output
	if outputDestination == "" {
		outputDestination = "replies"
	}
	c.Args = []string{
		"--inputs", function.Spec.Input,
		"--outputs", outputDestination,
		"--group", function.Name,
		"--protocol", function.Spec.Protocol,
		"--brokers", strings.Join(d.brokers, ","),
	}
	if needsVolume(function) {
		c.VolumeMounts = []corev1.VolumeMount{corev1.VolumeMount{Name: "pipes", MountPath: "/pipes"}}
	}
	return c
}

func needsVolume(function *v1.Function) bool {
	return function.Spec.Protocol == "pipes" || function.Spec.Protocol == "stdio"
}

func (d *deployer) Undeploy(function *v1.Function) error {
	propagation := metav1.DeletePropagationForeground
	return d.clientset.ExtensionsV1beta1().Deployments(function.Namespace).Delete(
		function.Name,
		&metav1.DeleteOptions{PropagationPolicy: &propagation})
}

func (d *deployer) Scale(function *v1.Function, replicas int) error {
	log.Printf("Scaling %v to %v", function.Name, replicas)

	deployment, err := d.clientset.ExtensionsV1beta1().Deployments(function.Namespace).Get(function.Name, metav1.GetOptions{})
	r := int32(replicas)
	deployment.Spec.Replicas = &r
	if err != nil {
		log.Printf("Could not scale %v: %v", function.Name, err)
		return err
	}
	_, err = d.clientset.ExtensionsV1beta1().Deployments(function.Namespace).Update(deployment)
	return err
}

func NewDeployer(config *rest.Config, brokers []string) (Deployer, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &deployer{clientset: clientset, brokers: brokers}, nil
}
