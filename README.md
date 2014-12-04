# Usher

## Installation

## Deployment

    container.deployModule("io.cozmic~usher~1.0-SNAPSHOT", {proxyConfig: {requestParsingRules: {type: "fixed", length: testMessage.length}}, responseParsingRules: {type: "fixed", length: testMessage.length}});
    

## Getting started on Mac

* Install [homebrew](http://brew.sh)

If your brew is old, then:

    brew update
    brew upgrade

Download vertx:

    http://vertx.io/downloads.html

Then:

    brew install maven vert.x groovy gradle

    export GROOVY_HOME=/usr/local/opt/groovy/libexec

## RocksDB

This is going to be a bit of a pain. Adds a bit of friction to getting started.

    git clone git@github.com:facebook/rocksdb.git
    cd rocksdb/
    git checkout rocksdb-3.2
    make rocksdbjava
    sudo cp librocksdb.a /usr/local/bin/
    

We'll need to build the library for the target platform and install in that platform's default system library path.

## What is usher

A tcp proxy (http-ready) that journals packets. It can be clustered using vertx clustering. The journal files though
are not "distributed". Each node keeps a separate journal. It "multiplexes" the incoming connections over a fixed
pool of connections to the target host. Good for "slow clients", e.g. devices and sensors.

## Why usher

 - Reduce burden of slow clients on app server.
 - Allow replay of missed data
 - Allow stream splitting to different backend hosts (not done)

## Starting usher

Take a look at echo_example.js.

    vertx run echo_example.js
    
This script starts usher using the built in echo server. You'll see that it deploys the usher module. The second
parameter is the configuration. The echo example defines a simple fixed length "Hello World" example. It does this
by defining a requestParsingRule object. Usher by default is a TCP proxy and the parsing rules define message boundaries.
Usher can also be used as an Http proxy, but that will need additional coding/testing.

Here's a more complete config example:

    {
        "proxyConfig": {
            "serviceHost": "localhost",
            "servicePort": 9191,
            "requestParsingRules": {
                   "type": "fixed",
                   "length": 2,
                   "nextRule": {
                       "type": "typeMap",
                       "typeMap": [
                           {
                               "bytes": ["1", "1"],
                               "length": 28,
                               "nextRule": {
                                   "type": "dynamicCount",
                                   "counterPosition": 26,
                                   "lengthPerSegment": 4
                               }
                           },
                           {
                               "bytes": ["2", "2"],
                               "length": 24,
                               "nextRule": {
                                   "type": "dynamicCount",
                                   "counterPosition": 32,
                                   "lengthPerSegment": 4
                               }
                           },
                           {
                               "bytes": ["3", "3"],
                               "length": 26
                           },
                           {
                               "bytes": ["4", "4"],
                               "length": 23
                           },
                           {
                               "bytes": ["7", "7"],
                               "length": 16,
                               "nextRule": {
                                   "type": "dynamicCount",
                                   "counterPosition": 14,
                                   "lengthPerSegment": 7
                               }
                           }
                       ]
                   }
               }
        },
        "responseParsingRules": {
            "type": "fixed",
            "length": 2,
            "nextRule": {
                "type": "typeMap",
                "typeMap": [
                    {
                        "bytes": ["3", "19"],
                        "length": 18
                    },
                    {
                        "bytes": ["1", "17"],
                        "length": 0
                    },
                    {
                        "bytes": ["2", "18"],
                        "length": 0
                    },
                    {
                        "bytes": ["7", "23"],
                        "length": 0
                    },
                    {
                        "bytes": ["0", "-1"],
                        "length": 0
                    }
                ]
            }
        },
        "persistence": {
            "journalerConfig": {},
            "timeoutLogConfig": {},
            "connectionLogConfig": {}
        },
        "shellConfig": {
                                "crash.auth":"simple",
                                "crash.auth.simple.username":"admin",
                                "crash.auth.simple.password":"admin",
                                "crash.ssh.port":2000
        }
    
    }
    
## proxyConfig.serviceHost
## proxyConfig.servicePort

Define the host and port of the proxy target. To load balance you can also add a "services" array:

    {
        "services": [
            {
                "host": "serverB",
                "port": 9192
        ]
    }
    
The plan is to make this dynamic and support service discovery in the near future.

## proxyConfig.requestParsingRules

Usher uses the RulesBasedPacketParser class from pulsar-core.

It allows for external configuration. More rules can be added, but this is all we needed right now.

It supports three rule types:

    Fixed Length - Statically specify the length of a segment
    Type Map - Map a byte array to a length
    Dynamic Count - Read a number at a specific location that indicates a repeating series of bytes and calculate the total length
    
Example:

    {
        "type": "fixed",
        "length": 2,
        "nextRule": {
            "type": "typeMap",
            "typeMap": [
                {
                    "bytes": ["1", "1"],
                    "length": 28,
                    "nextRule": {
                        "type": "dynamicCount",
                        "counterPosition": 26,
                        "lengthPerSegment": 4
                     }
                },
                {
                    "bytes": ["3", "3"],
                    "length": 26
                }
            ]
        }
    }
    
Notice how rules are chained in a linked list using the embedded nextRule. Each type entry in the typeMap
can have a different nextRule allowing for branching.

The rules for parsing the v2 Splitsecnd packets are in the config.json file in the usher project.

## responseParsingRules

Same rule engine used for request parsing, but this defines the response packets. Usher doesn't actually parse
response messages using these rules since packets get tagged with message IDs. However, usher uses pulsar to replay
timed out messages and to properly do that we must be able to parse responses.

## persistence

Can probably leave blank for now as database paths are hard coded. To store data files in a different location though, 
look more closely at this.

## shellConfig

(for now at least), Usher embeds mod-shell. We were going to use the "command parsing" framework found in that library
but it didn't work very well. I left it for now as it does have a few handy features.