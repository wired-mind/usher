package commands

import org.crsh.cli.Argument
import org.crsh.cli.Command
import org.crsh.cli.Option
import org.crsh.cli.Required
import org.crsh.cli.Usage
import org.crsh.command.ScriptException
import org.vertx.java.core.Handler
import org.vertx.java.core.eventbus.Message
import org.vertx.java.core.json.JsonObject
import org.vertx.mods.VertxCommand

@Usage("interact with proxies")
public class proxy extends VertxCommand {


    public proxy() {

    }

    @Command
    @Usage("list each proxy")
    public void list() {

        try {
            id = UUID.randomUUID().toString()


            def container = getContainer()

            def final logger = container.logger()
            def final ctx = context;
           // logger.info("Ready to send message")
            def responseHandler = new Handler<Message<String>>() {
                @Override
                void handle(Message<String> msg) {
                    System.out.println(msg.body());
                    ctx.provide([message: msg.body()])
                }
            }
            getVertx().eventBus().registerHandler(id, responseHandler);
            getVertx().setTimer(30000, new Handler<Long>() {
                @Override
                void handle(Long event) {
                    getVertx().eventBus().unregisterHandler(id, responseHandler);
                }
            })

            getVertx().eventBus().publish("io.cozmic.usher:proxy:list", id);
        }
        catch (Exception e) {
            throw new ScriptException("Could not list proxies $main: $e.message", e)
        }
    }


}