var vertx = require('vertx')
var console = require('vertx/console')
var container = require('vertx/container');

// This example will start the pulsar server and provider and then send an "abc" string to which will target the built
// in echo server.

var testMessage = "Hello World!";

container.deployModule("io.cozmic~pulsar-provider~1.0.0-SNAPSHOT", {port: 6061, responseParsingConfig: {type: "fixed", length: testMessage.length}, fakePersistenceHandler: {testMessage: testMessage}}, function(err, deployID) {
  if (!err) {
    console.log("Launched provider");

    container.deployModule("io.cozmic~pulsar-server~1.0.0-SNAPSHOT", {port: 6061}, function(err, deployID) {
      if (!err) {
        console.log("Launching an example mission that sends 1M requests to Usher on port 2500");
        vertx.eventBus.send("REQUEST_RANGE", {target: {host: "localhost", port:2500}, startingSequence: 1, endingSequence: 100000, maxBots: 100, concurrency: 30, rate: 0, delay: 0, pipelining: true})
      } else {
        console.log("Deployment failed! " + err);
      }
    });

  } else {
    console.log("Deployment failed! " + err);
  }
});


