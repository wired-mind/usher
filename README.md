# Usher

Guaranteed delivery net stream proxy

## Installation

The usherprotocols library contains classes that an app server can use to help with parsing usher streams.

    compile "io.cozmic:usherprotocols:1.0.0-SNAPSHOT"

## Deployment

    container.deployModule("io.cozmic~usher~1.0.0-SNAPSHOT", {proxyConfig: {host: "0.0.0.0", requestParsingRules: {type: "fixed", length: testMessage.length}}, responseParsingRules: {type: "fixed", length: testMessage.length}});
    
## Overview

  - [Introduction](#introduction)
  - [Running usher](#running-usher)
  - [Configure rocksdb](#configure-rocksdb)
  - [Examples](#examples)
  - [Configuration options](#configuration-options)
  - [Protocol](#protocol)
  - [Testing](#testing)
  - [Development](#development)
  
## Introduction

### What is usher

A tcp proxy (http-ready) that journals packets. It can be clustered using vertx clustering. The journal files though
are not "distributed". Each node keeps a separate journal. It "multiplexes" the incoming connections over a fixed
pool of connections to the target host. Good for "slow clients", e.g. devices and sensors.

### Why usher

 - Reduce burden of slow clients on app server.
 - Allow replay of missed data
 - Allow stream splitting to different backend hosts (not done)

## Running usher

Change the path of repos.txt to match your vertx installation.

    echo "maven:http://ec2-54-198-158-92.compute-1.amazonaws.com/content/groups/public/" >> /usr/local/Cellar/vert.x/2.1.2/libexec/conf/repos.txt
    vertx runmod "io.cozmic~usher~1.0.0-SNAPSHOT" -conf ./config.json 
    
Or just download the module from nexus.
 
## Configure rocksdb

This is going to be a bit of a pain. Adds a bit of friction to getting started. The c++ library must be built for the
target platform. These are the steps I used on my mac. The resulting library file needs to be in the java system
library path. I tried to setup gradle to dynamically configure the library path, but I had trouble doing it in a 
universal way. For now just add to the default system library path or provide the option on the command line.

    git clone git@github.com:facebook/rocksdb.git
    cd rocksdb/
    git checkout rocksdb-3.2
    make rocksdbjava
    sudo cp librocksdb.a /usr/lib/java/
    

We'll need to build the library for the target platform and install in that platform's default system library path.

## Examples

Take a look at echo_example.js.

    vertx run echo_example.js
    
This script starts usher using the built in echo server. You'll see that it deploys the usher module. The second
parameter is the configuration. The echo example defines a simple fixed length "Hello World" example. It does this
by defining a requestParsingRule object. Usher by default is a TCP proxy and the parsing rules define message boundaries.
Usher can also be used as an Http proxy, but that will need additional coding/testing.

Here's a more complete config example:

    {
        "proxyConfig": {
            "host": "0.0.0.0.",
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
    
## Configuration options

### proxyConfig.host
### proxyConfig.port

Defaults shown:

    "host": "localhost"
    "port": 2500

You will likely want to change the host binding to 0.0.0.0.

### proxyConfig.serviceHost
### proxyConfig.servicePort

Define the host and port of the proxy target. To load balance you can also add a "services" array:

    {
        "services": [
            {
                "host": "serverB",
                "port": 9192
        ]
    }
    
The plan is to make this dynamic and support service discovery in the near future.

### proxyConfig.requestParsingRules

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

### responseParsingRules

Same rule engine used for request parsing, but this defines the response packets. Usher doesn't actually parse
response messages using these rules since packets get tagged with message IDs. However, usher uses pulsar to replay
timed out messages and to properly do that we must be able to parse responses.

### persistence

Can probably leave blank for now as database paths are hard coded. To store data files in a different location though, 
look more closely at this.

### shellConfig

(for now at least), Usher embeds mod-shell. We were going to use the "command parsing" framework found in that library
but it didn't work very well. I left it for now as it does have a few handy features.

## Protocol

Writing an app server that uses usher requires a certain protocol. In summary, usher just prepends each message with a
"length", "id", connection id", and "timestamp". The app server should use the "id" to make services idempotent. App
servers should use the timestamp as the actual receive time of the message. Responses should include the "length" and
"id".

### Request byte order

    4   length
    4   messageId length
    36  messageId           //Doesn't have to be 36, but is right now for usher
    4   connectionId length
    36  connectionId        //Doesn't have to be 36, but is right now for usher
    8   timestamp
    N   original message

### Response byte order

    4   length
    4   messageId length
    36  messageId           // should correlate to the same id used in the request
    N   response

Usher users parsing libraries from pulsar. Usher also has an usherprotocol library with some higher level parsing classes.
The libraries just contains classes used for parsing usher streams, etc. They have vertx dependencies, but don't contain 
any modules or verticles. Just parsing classes, etc.

    compile "io.cozmic:usherprotocols:1.0.0-SNAPSHOT"
    compile "io.cozmic:pulsar-core:1.0.0-SNAPSHOT"
    
In a vertx application you can use the CozmicSocket and RuleBasedPacketSocket classes. These classes wrap a vertx
socket and parsing the incoming streams. They implement the vertx ReadStream interface so they can be used in Pumps.
You can look at the EchoChamber class as an example of using the CozmicSocket class:

    final CozmicSocket cozmicSocket = new CozmicSocket(socket);
    final Pump pump = Pump.createPump(cozmicSocket.translate(new CozmicStreamProcessor() {
        @Override
        public void process(Message message, AsyncResultHandler<Message> resultHandler) {
            try {
                final Request request = Request.fromEnvelope(message.buildEnvelope());
                final Buffer body = request.getBody();
                container.logger().info("Responding with: " + body.toString());
                final Message reply = message.createReply(body);
                resultHandler.handle(new DefaultFutureResult<>(reply));
            } catch (Exception ex) {
                resultHandler.handle(new DefaultFutureResult(ex));
            }
        }
    }), cozmicSocket);
    pump.start();
    
In a non vertx app server you might benefit from the CozmicParser and RuleBasedPacketParser classes in pulsar-core.
You can use these without a vertx socket just to parse "Buffer" objects.

    CozmicParser cozmicParser = new CozmicParser();
    cozmicParser.handler(new Handler<Buffer>() {
        @Override
        public void handle(Buffer buff) {
    
            int pos = 0;
            final int messageIdLength = buff.getInt(pos);
            pos += 4;
            String messageId = buff.getString(pos, pos + messageIdLength);
    
            pos += messageIdLength;
            final Buffer body = buff.getBuffer(pos, buff.length());
    
            readBuffers.add(new Message(messageId, body));
    
            if (paused) {
                return;
            }
    
            purgeReadBuffers();
        }
    });
    
    cozmicParser.handle(new Buffer(bytes));
    
You can chain the parsers too.

Also notice the Request class used above.

    final Request request = Request.fromEnvelope(message.buildEnvelope());
    
That will parse the final buffer into the "id", "connection id", "timestamp", etc.

## Testing

First start usher with the built in echo server.

    vertx run echo_example.js
    
Then you can run pulsar in a different vertx process to simulate lots of traffic for usher.

    vertx run load_test.js


## Development

At least for a mac. I'm not sure if this is current or not.

* Install [homebrew](http://brew.sh)

If your brew is old, then:

    brew update
    brew upgrade

Download vertx:

    http://vertx.io/downloads.html

Or:

    brew install maven vert.x groovy gradle

    export GROOVY_HOME=/usr/local/opt/groovy/libexec
