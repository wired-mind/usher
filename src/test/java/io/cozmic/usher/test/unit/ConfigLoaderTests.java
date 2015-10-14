package io.cozmic.usher.test.unit;

import io.cozmic.usher.ConfigLoader;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by chuck on 9/10/15.
 */
@RunWith(VertxUnitRunner.class)
public class ConfigLoaderTests {


    @Test
    public void canLoadDefault() {
        final ConfigLoader configLoader = ConfigLoader.usherDefault(new JsonObject());
        final JsonObject config = configLoader.buildUsherConfig();

        assertNotNull(config);
    }

    @Test
    public void canLoadWithPackage() {
        final ConfigLoader package1Loader = new ConfigLoader(new JsonObject()).withBasePackage("fakePackage").withConfigFile("test");
        final ConfigLoader package2Loader = new ConfigLoader(new JsonObject()).withBasePackage("fakePackage2").withConfigFile("test");
        final JsonObject config = package1Loader.buildUsherConfig();
        final JsonObject config2 = package2Loader.buildUsherConfig();

        assertEquals("Foo should equal bar", "bar", config.getString("foo"));
        assertEquals("Foo should equal bar2", "bar2", config2.getString("foo"));
        assertEquals("myGlobal should equal Hello", "Hello", config.getString("myGlobal"));
        assertEquals("myGlobal should equal Hello", "Hello", config2.getString("myGlobal"));
    }

    @Test
    public void canLoadWithPackageAndDefaultConf() {
        final ConfigLoader package1Loader = new ConfigLoader(new JsonObject()).withBasePackage("fakePackage");
        final ConfigLoader package2Loader = new ConfigLoader(new JsonObject()).withBasePackage("fakePackage2");
        final JsonObject config = package1Loader.buildUsherConfig();
        final JsonObject config2 = package2Loader.buildUsherConfig();

        assertEquals("Foo should equal bar", "bar", config.getString("foo"));
        assertEquals("Foo should equal bar2", "bar2", config2.getString("foo"));
        assertEquals("myGlobal should equal Hello", "Hello", config.getString("myGlobal"));
        assertEquals("myGlobal should equal Hello", "Hello", config2.getString("myGlobal"));
    }
}
