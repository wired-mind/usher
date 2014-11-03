package commands

import org.crsh.cli.Argument
import org.crsh.cli.Command
import org.crsh.cli.Option
import org.crsh.cli.Required
import org.crsh.cli.Usage
import org.crsh.command.ScriptException
import org.vertx.java.core.json.JsonObject
import org.vertx.mods.VertxCommand

@Usage("interact with echoChamber")
public class echoChamber extends VertxCommand {

    @Command
    @Usage("stop echoChamber")
    public void stop() {

        try {
         getVertx().eventBus().send("STOP_ECHO_CHAMBER", true);
        }
        catch (Exception e) {
            throw new ScriptException("Could not stop echoChamber $main: $e.message", e)
        }
    }

    @Command
    @Usage("start echoChamber")
    public void start() {

        try {
            getVertx().eventBus().send("START_ECHO_CHAMBER", true);
        }
        catch (Exception e) {
            throw new ScriptException("Could not start echoChamber $main: $e.message", e)
        }
    }
}