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

### KafkaOutput