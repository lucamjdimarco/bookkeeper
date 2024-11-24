package org.apache.bookkeeper.helper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class EntryBuilder {

    // builder per istanza valida
    public static ByteBuf createValidEntry() {
        long ledgerId = 1L;
        long entryId = 10L;
        long lastConfirmed = 9L;
        byte[] data = "ValidData".getBytes();
        byte[] authCode = "ValidAuth".getBytes();

        int metadataSize = Long.BYTES * 3; // ledgerId, entryId, lastConfirmed
        int dataSize = data.length;
        int authCodeSize = authCode.length;

        ByteBuf entry = Unpooled.buffer(metadataSize + dataSize + authCodeSize);

        // 1. metadati
        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        entry.writeLong(lastConfirmed);

        // 2. dati
        entry.writeBytes(data);

        // 3. codice di auth
        entry.writeBytes(authCode);

        return entry;
    }

    // builder per istanza non valida --> metadati non validi
    public static ByteBuf createInvalidEntry() {
        long ledgerId = -1L;
        long entryId = -1L;
        long lastConfirmed = -1L;
        byte[] data = "InvalidData".getBytes();
        byte[] authCode = "InvalidAuth".getBytes();


        int metadataSize = Long.BYTES * 3; // ledgerId, entryId, lastConfirmed
        int dataSize = data.length;
        int authCodeSize = authCode.length;

        ByteBuf entry = Unpooled.buffer(metadataSize + dataSize + authCodeSize);

        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        entry.writeLong(lastConfirmed);

        entry.writeBytes(data);
        entry.writeBytes(authCode);

        return entry;
    }

    // builder per istanza non valida --> metadati assenti
    public static ByteBuf createInvalidEntryWithoutMetadata() {
        byte[] data = "DataWithoutMetadata".getBytes();
        byte[] authCode = "MissingMetadata".getBytes();

        int dataSize = data.length;
        int authCodeSize = authCode.length;

        ByteBuf entry = Unpooled.buffer(dataSize + authCodeSize);
        entry.writeBytes(data);

        entry.writeBytes(authCode);

        return entry;
    }
}