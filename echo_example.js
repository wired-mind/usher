var vertx = require('vertx')
var console = require('vertx/console')
var container = require('vertx/container');

var testMessage = "Hello World!";


container.deployModule("io.cozmic~usher~1.0.1-SNAPSHOT", {proxyConfig: {requestParsingRules: {type: "fixed", length: testMessage.length}}, responseParsingRules: {type: "fixed", length: testMessage.length}}, function(err, deployID) {
  if (!err) {
    console.log("Launched Usher. Using Internal Echo Server. Configured for 12 byte fixed length messages");
  } else {
    console.log("Deployment failed! " + err);
  }
});


