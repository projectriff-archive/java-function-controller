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

package main

import (
	riffcs "github.com/projectriff/kubernetes-crds/pkg/client/clientset/versioned"
	informers "github.com/projectriff/kubernetes-crds/pkg/client/informers/externalversions"
	"k8s.io/client-go/tools/clientcmd"

	"flag"

	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/projectriff/function-controller/pkg/controller"
	informersV1 "github.com/projectriff/kubernetes-crds/pkg/client/informers/externalversions/projectriff/v1"
	"k8s.io/client-go/rest"
)

type Foo struct {
	Name string
}

func main() {

	kubeconfig := flag.String("kubeconf", "", "Path to a kube config. Only required if out-of-cluster.")
	masterURL := flag.String("master-url", "", "Path to master URL. Useful eg when using proxy")
	brokers := []string{os.Getenv("SPRING_CLOUD_STREAM_KAFKA_BINDER_BROKERS")} // TODO change to flag
	flag.Parse()
	config, err := clientcmd.BuildConfigFromFlags(*masterURL, *kubeconfig)
	if err != nil {
		log.Fatalf("Error getting client config: %s", err.Error())
	}

	topicsInformer, functionsInformer := makeInformers(config)
	deployer, err := controller.NewDeployer(config, brokers)
	if err != nil {
		panic(err)
	}
	ctrl := controller.NewController(topicsInformer, functionsInformer, deployer, controller.NewLagTracker(brokers))

	stopCh := make(chan struct{})
	go ctrl.Run(stopCh)

	// Trap signals to trigger a proper shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown
	<-signals
	log.Println("Shutting Down...")
	stopCh <- struct{}{}

}

func makeInformers(config *rest.Config) (informersV1.TopicInformer, informersV1.FunctionInformer) {
	riffClient, err := riffcs.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error building riff clientset: %s", err.Error())
	}
	riffInformerFactory := informers.NewSharedInformerFactory(riffClient, 0)
	topicsInformer := riffInformerFactory.Projectriff().V1().Topics()
	functionsInformer := riffInformerFactory.Projectriff().V1().Functions()
	return topicsInformer, functionsInformer
}
