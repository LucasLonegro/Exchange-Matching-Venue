package io.lmb.common.access;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OperatorTokensTest
{
    @TempDir
    Path dir;

    @Test
    void eachTokenNamesItsOperator() throws Exception
    {
        final Path f = dir.resolve("operators");
        Files.writeString(f, "# name sha256\nalice " + AccessDirectory.sha256Hex("tok-a") + "\n\nbob   " +
            AccessDirectory.sha256Hex("tok-b") + "\n");
        final OperatorTokens ops = OperatorTokens.load(f);
        assertEquals("alice", ops.operator("tok-a"));
        assertEquals("bob", ops.operator("tok-b"));
        assertNull(ops.operator("tok-c"));
        assertNull(ops.operator(""));
        assertNull(ops.operator(null));
        assertEquals("admin", OperatorTokens.single("shared").operator("shared"));

        Files.writeString(f, "alice tok-a-in-clear\n");
        assertThrows(IllegalArgumentException.class, () -> OperatorTokens.load(f), "plaintext token");
        Files.writeString(f, "alice " + AccessDirectory.sha256Hex("x") + "\nbob " + AccessDirectory.sha256Hex("x") + "\n");
        assertThrows(IllegalArgumentException.class, () -> OperatorTokens.load(f), "shared token");
        Files.writeString(f, "# nobody\n");
        assertThrows(IllegalArgumentException.class, () -> OperatorTokens.load(f), "empty");
    }
}
