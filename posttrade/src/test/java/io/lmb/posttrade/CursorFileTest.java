package io.lmb.posttrade;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorFileTest
{
    @TempDir
    Path dir;

    @Test
    void roundTripsAndRejectsCorruptionAndTornWrites() throws IOException
    {
        final Path f = dir.resolve("c.ckpt");
        assertTrue(CursorFile.read(f).isEmpty());
        final Map<String, String> m = new LinkedHashMap<>();
        m.put("shard.0", "42");
        m.put("session.FIRM100", "0:12;1:7");
        CursorFile.write(f, m);
        assertEquals(m, CursorFile.read(f));

        Files.writeString(f, Files.readString(f).replace("42", "43"));
        assertThrows(IOException.class, () -> CursorFile.read(f));
        Files.writeString(f, "shard.0=42\n");
        assertThrows(IOException.class, () -> CursorFile.read(f));
    }
}
