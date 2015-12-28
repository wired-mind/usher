# Usher

Usher is inspired [Heka's](https://hekad.readthedocs.org). The big differences at a glance:

    - usher has a limited number of plugins at this point, Tcp and Kafka
    - usher uses Avro
    - usher is bidirectional
    - usher leverages vertx for concurrency

Usher is better able to support application layer type services because of bidirectional data flow and enough controls
over kafka commits.

## Mux

Usher demuxes data from inputs to one or more outputs or filters. Bidirectional responses from the outputs or filters
are muxed back to the input. The mux applies routing logic to messages using the [Message matcher].

## Config

We use [typesafe config](https://github.com/typesafehub/config).

### Global config values

        usher {
            pipelineInstances: 1    // defaults to CPU cores. Controls how many verticles run concurrently
            minIdle: 1              // defaults to 3. Can be set to -1 to disable the mux pool
            validationInterval: 60  // defaults to 60s. Recycles the mux pool on this interval.
        }

## Inputs

### TcpInput

### KafkaInput

        KafkaInput {
            type: KafkaInput
            topic: my-topic
            "group.id": my-service
            splitter: KafkaSplitter
            decoder: MyDecoder
            "seed.brokers": kafka-1,kafka-2,kafka-3
            "bootstrap.servers": kafka-1,kafka-2,kafka-3
            numberOfThreads: 100 #Defaults to 10
        }

        KafkaSplitter: { "type": "io.cozmic.usher.plugins.core.NullSplitter", "useMessageBytes": true}


numberOfThreads - this setting (defaults to 10) controls how many partitions can be processed concurrently. I.e. If there
are 100 partitions, by default, then each instance of this input would only process 10 out of the 100 partitions. So for
maximum concurrency you would set numberOfThreads to 100 OR for maximum parallelism you would run 10 instances of usher
on 10 different machines. Theoretically this is how it works, however, more testing is required to verify proper implementation.

## Message matcher

See JUEL

## Common settings for filters and outputs

        myOutput {
            errorStrategy: {
                type: rethrow | ignore | retry
                #If retry then...
                maxRetries: 10 //defaults to -1 for infinite
                maxDelayMillis: 10000 // default 10000ms to cap exponential backoff
                exponentialBackoff: true | false
                retryDelayMillis: 500 // defaults to 1000ms
                retryDelayMultiplier: 2 //defaults to 1.5
            }
        }

## Filters

Filters let you process data within usher instead of (or in addition to) sending to an Output.

Filters allow for a timeout settings.

        myFilter {
            timeout: 10000 // defaults to -1, no timeout. An exception is raised if a filter isn't complete within the timeout.
        }

### AbstractFilter

AbstractFilter is a base class for creating simplified filters. Concrete instances only need to implement the handleRequest
method which provides asynchronous request/reply semantics as well as a messageInjector which can be used to send new
messages to the mux.

## Outputs

### TcpOutput

        TcpOutput {
           type: "TcpOutput"
           host: localhost
           port: 8080
           useFraming: true
           splitter: "UsherV1FramingSplitter"
           encoder: "PayloadEncoder"
           messageMatcher: "#{1==1}"
           reconnectAttempts: -1 # means always reconnect
           reconnectInterval: 3000
         }

New: keepAlive is true by default. If set to false you will force close the socket after each message is sent. When false
the socket won't be bi-directional. Any data received by the socket is ignored.

        keepAlive=false

### KafkaOutput

        KafkaOutput {
            bootstrap.servers=kafka.dev:9092
            topic=mytopic
            key=mykey
        }

Topic is required. Key is optional.

Both topic and key support expressions. The expression variables available are the PipelinePack. This means that if
your message is a POJO, i.e. Person. You could dynamically create a topic, e.g.

        topic=#{pack.message.gender}

This would publish the messages to a topic that evaluated to the person's gender.

Similarly you can can make key dynamic (not sure a static key makes much sense anyway).

        key=#{pack.message.lastName}

If key is omitted, then Kafka will use round-robin partitioning. Because usher's KafkaInput plugin is concurrent
round-robin partitioning can lead to out-of-order messages. So be sure to use a partitioning key accordingly.