package io.cozmic.usher.pipeline;

import de.odysseus.el.util.SimpleContext;
import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.net.SocketAddress;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

/**
 * Created by chuck on 7/3/15.
 */
public class JuelMatcher implements MessageMatcher {

    private static ExpressionFactory factory = ExpressionFactory.newInstance();
    private SimpleContext context = new SimpleContext();
    private ValueExpression expression;

    public JuelMatcher(String expressionVal) {
        expression = factory.createValueExpression(context, expressionVal, boolean.class);
    }

    @Override
    public boolean matches(PipelinePack pipelinePack) {
        SimpleContext runtimeContext = new SimpleContext();

        //This needs some improvement
        if (pipelinePack.getMessage() instanceof Message) {
            Message message = pipelinePack.getMessage();
            factory.createValueExpression(runtimeContext, "${localPort}", int.class).setValue(runtimeContext, message.getLocalAddress().port());
            factory.createValueExpression(runtimeContext, "${localHost}", String.class).setValue(runtimeContext, message.getLocalAddress().host());
            factory.createValueExpression(runtimeContext, "${remotePort}", int.class).setValue(runtimeContext, message.getRemoteAddress().port());
            factory.createValueExpression(runtimeContext, "${remoteHost}", String.class).setValue(runtimeContext, message.getRemoteAddress().host());
        }

        return (boolean) expression.getValue(runtimeContext);
    }
}
