package io.cozmic.usher.pipeline;

import de.odysseus.el.util.SimpleContext;
import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.message.PipelinePack;

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
        return (boolean) expression.getValue(pipelinePack.getRuntimeContext());
    }
}
