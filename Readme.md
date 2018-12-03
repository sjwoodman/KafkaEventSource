# Knative KafkaEventSource

This deploys an Operator that can instantiate Knative Event Sources which subscribe to a Kafka Topic.

## EventSource

The image that receives messages from Kafka (Receive Adaptor) is available at `docker.io/sjwoodman/kafkaeventsource`.
The build it from source edit the `eventsource/Makefile` with suitable tags and perform the following commands.

```bash
make docker_build docker_push_latest
```

## Usage

The sample connects to a [Strimzi](http://strimzi.io/quickstarts/okd/) Kafka Broker running inside OpenShift.

1. Setup [Knative Eventing](https://github.com/knative/docs/tree/master/eventing).

1. Install the [in-memory `ClusterChannelProvisioner`](https://github.com/knative/eventing/tree/master/config/provisioners/in-memory-channel).
    - Note that you can skip this if you choose to use a different type of `Channel`. If so, you will need to modify `01-channel.yaml` before deploying it.

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

1. (Optional) Create a Topic in Strimzi to back the Channel.
    If you don't do this, defaults will be used for the number of partitions etc.

    ```bash
    kubectl create -f sample/00-topic.yaml
    ```

1. Create a KafkaEventSource and wire it up to a function. Either

    ```bash
    kubectl create -f sample/01-channel.yaml
    kubectl create -f sample/02-eventsource.yaml
    kubectl apply -f sample/03-service.yaml -n myproject
    kubectl create -f sample/subscription.yaml -n myproject
    ```

1. Verify that the EventSource has been started

    ```bash
    $ kubectl get pods
    NAME                                          READY     STATUS    RESTARTS   AGE
    example-kafkaeventsource-6b6477f95d-tc4nd     2/2       Running   1          2m
    ```

1. Send some Kafka messages

    ```bash
    $ oc exec -it my-cluster-kafka-0 -- bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input
    > test message
    ```