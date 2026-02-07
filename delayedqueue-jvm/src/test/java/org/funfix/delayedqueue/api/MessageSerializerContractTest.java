/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.api;

import org.funfix.delayedqueue.jvm.MessageSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

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
            public byte[] serialize(Integer payload) {
                return payload.toString().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Integer deserialize(byte[] serialized) {
                String decoded = new String(serialized, StandardCharsets.UTF_8);
                if ("INVALID".equals(decoded)) {
                    throw new IllegalArgumentException("Cannot parse INVALID as Integer");
                }
                return Integer.parseInt(decoded);
            }
        };

        // Should succeed for valid input
        assertEquals(42, serializer.deserialize("42".getBytes(StandardCharsets.UTF_8)));

        // Should throw IllegalArgumentException for invalid input
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> serializer.deserialize("INVALID".getBytes(StandardCharsets.UTF_8)),
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
            public byte[] serialize(String payload) {
                return ("PREFIX:" + payload).getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(byte[] serialized) {
                String decoded = new String(serialized, StandardCharsets.UTF_8);
                if (!decoded.startsWith("PREFIX:")) {
                    throw new IllegalArgumentException("Missing PREFIX");
                }
                return decoded.substring(7);
            }
        };

        assertEquals("custom.Type", custom.getTypeName());
        assertArrayEquals("PREFIX:test".getBytes(StandardCharsets.UTF_8), custom.serialize("test"));
        assertEquals("test", custom.deserialize("PREFIX:test".getBytes(StandardCharsets.UTF_8)));

        assertThrows(IllegalArgumentException.class,
            () -> custom.deserialize("INVALID".getBytes(StandardCharsets.UTF_8)));
    }
}
