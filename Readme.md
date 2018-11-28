# Knative KafkaEventSource

This deploys an Operator that can instantiate Knative Event Sources which subscribe to a Kafka Topic

## Usage

The sample connects to a [Strimzi](http://strimzi.io/quickstarts/okd/) Kafka Broker running inside OpenShift.

1. Setup [Knative Eventing](https://github.com/knative/docs/tree/master/eventing).

1. Install the [in-memory `ClusterChannelProvisioner`](https://github.com/knative/eventing/tree/master/config/provisioners/in-memory-channel).
    - Note that you can skip this if you choose to use a different type of `Channel`. If so, you will need to modify `channel.yaml` before deploying it.

1. Install Knative Serving and Eventing 

1. Install the KafkaEventSource CRD

    ```bash
    kubectl create -f deploy/crds/sources_v1alpha1_kafkaeventsource_crd.yaml
    ```

1. Setup RBAC and deploy the KafkaEventSource-operator:
   
    ```bash
    kubectl create -f deploy/service_account.yaml
    kubectl create -f deploy/role.yaml
    kubectl create -f deploy/role_binding.yaml
    kubectl create -f deploy/operator.yaml
    ```

1. Verify that the KafkaEventSource-operator is up and running:

    ```bash
    $ kubectl get deployment
    NAME                              DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
    kafkaeventsource-operator         1         1         1            1           2m
    ```
   
1. Create a KafkaEventSource and wire it up to a function. Either

    ```bash
    ko apply -f sample/deploy.yaml
    #or 
    kubectl create -f sample/01-channel.yaml
    kubectl create -f sample/02-eventsource.yaml
    ko apply -f sample/03-service.yaml
    kubectl create -f sample/subscription.yaml
    ```

1. Verify that the EventSource has been started

    ```bash
    $ kubectl get pods                           
    NAME                                          READY     STATUS    RESTARTS   AGE
    example-kafkaeventsource-6b6477f95d-tc4nd     2/2       Running   1          2m
    ```

1. Send some Kafka messages

    ```bash
    oc exec -it my-cluster-kafka-0 -- bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input
    > test message
    ```