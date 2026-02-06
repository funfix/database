package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.MessageSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MessageSerializer API contract.
 */
public class MessageSerializerContractTest {

    @Test
    public void testForStringsHasTypeName() {
        MessageSerializer<String> serializer = MessageSerializer.forStrings();
        
        assertNotNull(serializer.getTypeName(), "typeName must not be null");
        assertFalse(serializer.getTypeName().isEmpty(), "typeName must not be empty");
        assertEquals("java.lang.String", serializer.getTypeName(), 
            "String serializer should report java.lang.String as type name");
    }

    @Test
    public void testDeserializeFailureThrowsIllegalArgumentException() {
        MessageSerializer<Integer> serializer = new MessageSerializer<Integer>() {
            @Override
            public String getTypeName() {
                return "java.lang.Integer";
            }

            @Override
            public String serialize(Integer payload) {
                return payload.toString();
            }

            @Override
            public Integer deserialize(String serialized) {
                if ("INVALID".equals(serialized)) {
                    throw new IllegalArgumentException("Cannot parse INVALID as Integer");
                }
                return Integer.parseInt(serialized);
            }
        };

        // Should succeed for valid input
        assertEquals(42, serializer.deserialize("42"));

        // Should throw IllegalArgumentException for invalid input
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> serializer.deserialize("INVALID"),
            "deserialize must throw IllegalArgumentException for invalid input"
        );
        
        assertTrue(exception.getMessage().contains("INVALID"), 
            "Exception message should mention the invalid input");
    }

    @Test
    public void testCustomSerializerContract() {
        MessageSerializer<String> custom = new MessageSerializer<String>() {
            @Override
            public String getTypeName() {
                return "custom.Type";
            }

            @Override
            public String serialize(String payload) {
                return "PREFIX:" + payload;
            }

            @Override
            public String deserialize(String serialized) {
                if (!serialized.startsWith("PREFIX:")) {
                    throw new IllegalArgumentException("Missing PREFIX");
                }
                return serialized.substring(7);
            }
        };

        assertEquals("custom.Type", custom.getTypeName());
        assertEquals("PREFIX:test", custom.serialize("test"));
        assertEquals("test", custom.deserialize("PREFIX:test"));
        
        assertThrows(IllegalArgumentException.class, 
            () -> custom.deserialize("INVALID"));
    }
}
