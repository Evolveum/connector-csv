package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.security.GuardedByteArray;

/**
 * Created by Viliam Repan (lazyman).
 */
public class ByteArrayAccessor implements GuardedByteArray.Accessor {

    private byte[] value;

    @Override
    public void access(byte[] bytes) {
        value = bytes;
    }

    public byte[] getValue() {
        return value;
    }
}
