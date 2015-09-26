package io.cozmic.usher.test.unit;

import com.google.common.io.Resources;
import io.cozmic.usher.core.AvroMapper;
import io.cozmic.usher.test.Pojo;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static junit.framework.TestCase.assertNotNull;

/**
 * Created by chuck on 9/25/15.
 */
@RunWith(VertxUnitRunner.class)
public class AvroMapperTests {

    @Test
    public void canSerializePojo() throws IOException {
        final AvroMapper avroMapper = new AvroMapper(Resources.getResource("avro/pojo.avsc").openStream());
        final byte[] bytes = avroMapper.serialize(new Pojo("test", "test", 1, "blue"));
        assertNotNull(bytes);
    }
}
