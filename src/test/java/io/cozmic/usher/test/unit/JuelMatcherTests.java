package io.cozmic.usher.test.unit;

import de.odysseus.el.util.SimpleContext;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.net.impl.SocketAddressImpl;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by chuck on 9/10/15.
 */
@RunWith(VertxUnitRunner.class)
public class JuelMatcherTests {

    /**
     * Not really testing our code. Added this to confirm expected behavior of our EL library.
     */
    @Test
    public void canFilterUsingPojo() {
        SimpleContext context = new SimpleContext();
         ExpressionFactory factory = ExpressionFactory.newInstance();
        ValueExpression expression = factory.createValueExpression(context, "#{msg.localPort == 123}", boolean.class);

        SimpleContext runtimeContext = new SimpleContext();

        final Message message = new Message();
        message.setLocalPort(123);

        factory.createValueExpression(runtimeContext, "${msg}", Object.class).setValue(runtimeContext, message);


        assertTrue((boolean) expression.getValue(runtimeContext));
    }
}
