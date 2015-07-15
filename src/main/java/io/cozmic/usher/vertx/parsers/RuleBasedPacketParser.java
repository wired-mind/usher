package io.cozmic.usher.vertx.parsers;


import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by chuck on 11/14/14.
 *
 * Supply this class a set of rules formatted as json. The ruleset is a "linked list" and allows for progressive parsing.
 * The purpose of each rule is to determine how many bytes to read next. After all rules are processed, the resulting
 * buffer is returned.
 */
public class RuleBasedPacketParser implements Handler<Buffer> {

    private Handler<Buffer> outputHandler;
    protected RecordParser innerParser;
    private Rule currentRule;
    private Buffer buff;


    public static RuleBasedPacketParser fromConfig(JsonObject config, Handler<Buffer> output) {
        Objects.requireNonNull(config, "config");

        return new RuleBasedPacketParser(config, output);
    }



    private RuleBasedPacketParser(JsonObject config, final Handler<Buffer> output) {
        this.outputHandler = output;

        final Rule firstRule = Rule.build(config);
        currentRule = firstRule;

        innerParser = RecordParser.newFixed(currentRule.length(null), new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                if (buff == null) {
                    buff = buffer;
                } else {
                    buff.appendBuffer(buffer);
                }
                if (currentRule == null) {
                    outputHandler.handle(buff);
                    buff = null;
                    currentRule = firstRule;
                }

                final int nextLength = currentRule.length(buffer);
                if (nextLength == 0) {
                    outputHandler.handle(buff);
                    buff = null;
                    currentRule = firstRule;
                    innerParser.fixedSizeMode(currentRule.length(null));
                    currentRule = currentRule.nextRule(null);
                    return;
                }

                innerParser.fixedSizeMode(nextLength);
                currentRule = currentRule.nextRule(buffer);
            }
        });

        currentRule = currentRule.nextRule(null);
    }

    @Override
    public void handle(Buffer buffer) {
        innerParser.handle(buffer);
    }

    public void setOutput(Handler<Buffer> output) {
        Objects.requireNonNull(output, "output");
        this.outputHandler = output;
    }

    /**
     * Simple rule with a hardcoded length. Defaults to 1 byte.
     */
    private static class FixedRule extends Rule {

        private final int length;

        public FixedRule(JsonObject config) {
            super(config);
            length = config.getInteger("length", 1);
        }

        @Override
        public int length(Buffer buffer) {
            return length;
        }

        @Override
        public Rule nextRule(Buffer buffer) {
            return nextRule;
        }

    }


    /**
     * An array of objects that map a "byte array" to a length and the next rule. I.e. {"bytes": ["1", "3"], "length": 28, "nextRule": {}} Represent each
     * btye in json with the decimal equivalent string. See documentation for Java's Byte.parseByte(String)
     */
    private static class TypeMapRule extends Rule {
        Map<String, Integer> mapOfLengths = new HashMap<>();
        Map<String, Rule> mapOfNextRules = new HashMap<>();
        private final String keyEncoding = "UTF-8";
        private final Charset keyEncodingCharset = Charset.forName("UTF-8");

        public TypeMapRule(JsonObject config) {
            super(config);
            final JsonArray typeMap = config.getJsonArray("typeMap");
            if (typeMap == null) {
                throw new RuntimeException("Expecting an array of map objects ");
            }
            final List mapEntries = typeMap.getList();
            for (Object objEntry : mapEntries) {
                final JsonObject entry = (objEntry instanceof JsonObject) ? (JsonObject)objEntry : new JsonObject((Map<String, Object>) objEntry);
                final Integer length = entry.getInteger("length");
                final JsonObject nextRuleConfig = entry.getJsonObject("nextRule", new JsonObject());
                final JsonArray byteArray = entry.getJsonArray("bytes");
                if (length == null || byteArray == null) {
                    throw new RuntimeException("Expecting an array of bytes. Bytes and length are required for each mapping.");
                }

                final List byteList = byteArray.getList();
                final int keySize = byteList.size();
                byte[] key = new byte[keySize];
                for (int i = 0; i < keySize; i++) {
                    final Object objByte = byteList.get(i);
                    key[i] = Byte.parseByte((String) objByte);
                }

                final String encodedKey = new String(key, keyEncodingCharset);
                mapOfLengths.put(encodedKey, length);
                mapOfNextRules.put(encodedKey, Rule.build(nextRuleConfig));
            }
        }

        /**
         * Assumes that the buffer bytes are the key to the type map. This key determines the next segment length
         * and the next rule to process if any.
         * @param buffer
         * @return Length defined in typemap for the specified key
         */
        @Override
        public int length(Buffer buffer) {
            final String key = buffer.getString(0, buffer.length(), keyEncoding);
            final Integer length = mapOfLengths.get(key);
            return length != null ? length : 0;
        }

        /**
         * Assumes that the buffer bytes are the key to the type map. This key determines the next rule.
         * @param buffer
         * @return Next rule based on the key for the typeMap
         */
        @Override
        public Rule nextRule(Buffer buffer) {
            final String key = buffer.getString(0, buffer.length(), keyEncoding);

            return mapOfNextRules.get(key);
        }

    }

    /**
     * Computes a dynamic segment length based on a count/size field. I.e.
     * {"counterPosition": 28, "counterSize": 2, "lengthPerSegment": 4}
     *
     * Where counterSize is the number of bytes storing the counter (2=short, 4=int, 8=long, etc)
     *
     * counterPosition defaults to 0
     * counterSize defaults to 2 (short)
     */
    private static class DynamicCountRule extends Rule {

        private final int counterSize;
        private final int counterPosition;
        private final Integer lengthPerSegment;

        public DynamicCountRule(JsonObject config) {
            super(config);
            counterPosition = config.getInteger("counterPosition", 0);
            counterSize = config.getInteger("counterSize", 2);
            lengthPerSegment = config.getInteger("lengthPerSegment");

            if (lengthPerSegment == null) {
                throw new IllegalArgumentException("Length per segment is required");
            }

            final boolean validCounterSize = counterSize == 1 || counterSize == 2 || counterSize == 4 || counterSize == 8;
            if (!validCounterSize) {
                throw new IllegalArgumentException("Counter size must be 1, 2, 4, or 8");
            }

        }

        @Override
        public int length(Buffer buffer) {
            long count = 0;
            switch (counterSize) {
                case 1:
                    count = buffer.getByte(counterPosition) & 0xFF;
                    break;
                case 2:
                    count = (long) buffer.getShort(counterPosition);
                    break;
                case 4:
                    count = (long) buffer.getInt(counterPosition);
                    break;
                case 8:
                    count = buffer.getLong(counterPosition);
                    break;
            }

            return (int) (count * lengthPerSegment);
        }

        @Override
        public Rule nextRule(Buffer buffer) {
            return nextRule;
        }
    }

    private static abstract class Rule {


        protected final Rule nextRule;

        public Rule(JsonObject config) {
            nextRule = build(config.getJsonObject("nextRule", new JsonObject()));
        }



        public static Rule build(JsonObject config) {
            if (config == null) {
                return null;
            }

            final String type = config.getString("type");
            if (type == null) {
                return null;
            }
            switch (type) {
                case "fixed":
                    return new FixedRule(config);
                case "typeMap":
                    return new TypeMapRule(config);
                case "dynamicCount":
                    return new DynamicCountRule(config);

                default:
                    throw new RuntimeException("Rule type not supported");
            }
        }

        public abstract int length(Buffer buffer);


        public abstract Rule nextRule(Buffer buffer);
    }
}
