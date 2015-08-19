package io.cozmic.usher.test.unit;

import io.cozmic.usher.core.ObjectPool;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by chuck on 8/19/15.
 */
@RunWith(VertxUnitRunner.class)
public class ObjectPoolTests {

    @Test
    public void canCreateFakeObjectPool() {
        FakeObjectPool fakePool = new FakeObjectPool(0);
        Assert.assertNotNull(fakePool);
    }

    @Test
    public void canBorrowObjectFromPoolInitializedWithNoObjects(TestContext context) {
        FakeObjectPool fakePool = new FakeObjectPool(0);

        fakePool.borrowObject(context.asyncAssertSuccess(obj -> {
            context.assertNotNull(obj);
            context.assertEquals(0, fakePool.getPoolSize(), "Expect pool size to be zero after borrowing");
        }));
    }

    @Test
    public void canBorrowObjectFromPoolInitializedWithOneObjects(TestContext context) {
        FakeObjectPool fakePool = new FakeObjectPool(1);

        fakePool.borrowObject(context.asyncAssertSuccess(obj -> {
            context.assertNotNull(obj);
            context.assertEquals(0, fakePool.getPoolSize(), "Expect pool size to be zero after borrowing");
        }));
    }

    @Test
    public void canBorrowObjectFromPoolInitializedWithTwoObjects(TestContext context) {
        FakeObjectPool fakePool = new FakeObjectPool(2);

        fakePool.borrowObject(context.asyncAssertSuccess(obj -> {
            context.assertNotNull(obj);
            context.assertEquals(1, fakePool.getPoolSize(), "Expect pool size to be one after borrowing from pool initialized with 2");
        }));
    }

    @Test
    public void canReturnBorrowedObjectFromPoolInitializedWithTwoObjects(TestContext context) {
        FakeObjectPool fakePool = new FakeObjectPool(2);

        fakePool.borrowObject(context.asyncAssertSuccess(obj -> {
            context.assertNotNull(obj);
            context.assertEquals(1, fakePool.getPoolSize(), "Expect pool size to be one after borrowing from pool initialized with 2");

            fakePool.returnObject(obj);

            context.assertEquals(2, fakePool.getPoolSize(), "Expect pool size to be two after returning borrowed object from pool initialized with 2");
        }));
    }

    /**
     * MinIdle of -1 is a special case that prevents objects from staying in the pool
     *
     * @param context
     */
    @Test
    public void canReturnBorrowedObjectFromPoolInitializedWithNegativeOne(TestContext context) {
        FakeObjectPool fakePool = new FakeObjectPool(-1);

        fakePool.borrowObject(context.asyncAssertSuccess(obj -> {
            context.assertNotNull(obj);
            context.assertEquals(0, fakePool.getPoolSize(), "Expect pool size to be zero after borrowing from pool initialized with -1");

            fakePool.returnObject(obj);

            context.assertEquals(0, fakePool.getPoolSize(), "Expect pool size to be zero after returning borrowed object from pool initialized with -1");

            context.assertTrue(obj.isDestroyed(), "Object should be destroyed after returning to pool when minIdle is -1");
        }));
    }

    private class FakeObjectPool extends ObjectPool<Fake> {
        public FakeObjectPool(int minIdle) {
            super(minIdle);
        }

        @Override
        protected Class className() {
            return getClass();
        }

        @Override
        protected void destroyObject(Fake obj) {
            obj.setDestroyed(true);
        }

        @Override
        protected void createObject(AsyncResultHandler<Fake> readyHandler) {
            readyHandler.handle(Future.succeededFuture(new Fake()));

        }
    }


    private class Fake {
        private boolean destroyed;

        public boolean isDestroyed() {
            return destroyed;
        }

        public void setDestroyed(boolean destroyed) {
            this.destroyed = destroyed;
        }
    }
}
