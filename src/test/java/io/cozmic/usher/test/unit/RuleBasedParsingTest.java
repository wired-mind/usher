package io.cozmic.usher.test.unit;



import io.cozmic.usher.vertx.parsers.PacketParsingException;
import io.cozmic.usher.vertx.parsers.RuleBasedPacketParser;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Created by chuck on 11/14/14.
 */
public class RuleBasedParsingTest {

    @Test
    public void testBasicParsing() throws URISyntaxException, IOException {
        final URI uri = getClass().getResource("/example_request_parsing_rules.json").toURI();
        final String configString = new String(Files.readAllBytes(Paths.get(uri)));


        final AtomicInteger counter = new AtomicInteger();
        final RuleBasedPacketParser parser = RuleBasedPacketParser.fromConfig(new JsonObject(configString), new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                counter.incrementAndGet();
            }
        });

        int packetCount = 10;
        Buffer sampleBuffer = Buffer.buffer();
        for (int i = 0; i < packetCount; i++) {
            sampleBuffer.appendBuffer(buildBlankStartupPacket());
        }
        parser.handle(sampleBuffer);

        assertEquals(packetCount, counter.intValue());
    }


    @Test
    public void testMixedPacketParsing() throws URISyntaxException, IOException {
        final URI uri = getClass().getResource("/example_request_parsing_rules.json").toURI();
        final String configString = new String(Files.readAllBytes(Paths.get(uri)));


        final AtomicInteger counter = new AtomicInteger();
        final RuleBasedPacketParser parser = RuleBasedPacketParser.fromConfig(new JsonObject(configString), new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                counter.incrementAndGet();
            }
        });

        int packetCount = 10;
        Buffer sampleBuffer = Buffer.buffer();
        for (int i = 0; i < packetCount; i++) {
            sampleBuffer.appendBuffer(buildBlankStartupPacket());
            sampleBuffer.appendBuffer(buildBlankTrackingPacket(5));
            sampleBuffer.appendBuffer(buildBlankTrackingPacket(0));
            sampleBuffer.appendBuffer(buildBlankStartupPacket());
            sampleBuffer.appendBuffer(buildBlankStartupPacket());
            sampleBuffer.appendBuffer(buildBlankTrackingPacket(5));
            sampleBuffer.appendBuffer(buildBlankStartupPacket());
            sampleBuffer.appendBuffer(buildBlankTrackingPacket(0));
        }
        parser.handle(sampleBuffer);

        assertEquals(packetCount * 8, counter.intValue());
    }

    @Test
    public void testIncompletePacketParsing_shouldError() throws URISyntaxException, IOException {
        final URI uri = getClass().getResource("/example_request_parsing_rules.json").toURI();
        final String configString = new String(Files.readAllBytes(Paths.get(uri)));


        final AtomicInteger counter = new AtomicInteger();
        final RuleBasedPacketParser parser = RuleBasedPacketParser.fromConfig(new JsonObject(configString), new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                counter.incrementAndGet();
            }
        });

        Buffer sampleBuffer = Buffer.buffer();
        sampleBuffer.appendBuffer(buildBlankStartupPacket());
        sampleBuffer.appendBuffer(buildBlankTrackingPacket(5));
        sampleBuffer.appendBuffer(buildIncompleteTrackingPacket(3));
        sampleBuffer.appendBuffer(buildBlankStartupPacket());
        
        try {
        	parser.handle(sampleBuffer);
        } catch( PacketParsingException e ) {
        	assertTrue(true);
        	return;
        }
    	
        assertFalse(true);  //If error is not thrown, then the test is bad
    }
    
    private Buffer buildBlankTrackingPacket(int dynamicSegmentCount) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) 0x01);
        buffer.appendByte((byte) 0x01);
        buffer.appendBytes(new byte[26]);
        buffer.appendShort((short) dynamicSegmentCount);
        buffer.appendBytes(new byte[dynamicSegmentCount * 4]);
        return buffer;
    }

    private Buffer buildIncompleteTrackingPacket(int dynamicSegmentCount) {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) 0x01);
        buffer.appendByte((byte) 0x01);
        buffer.appendBytes(new byte[26]);
        buffer.appendShort((short) dynamicSegmentCount);
        buffer.appendBytes(new byte[(dynamicSegmentCount-1) * 4]);
        return buffer;
    }

    private Buffer buildBlankStartupPacket() {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) 0x03);
        buffer.appendByte((byte) 0x03);
        buffer.appendBytes(new byte[26]);
        return buffer;
    }
}
