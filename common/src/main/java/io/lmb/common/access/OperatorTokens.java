package io.lmb.common.access;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Who may call an admin REST API. Each operator has their own token; the file lists {@code <name> <sha256 of the
 * token>} per line ({@code #} comments), so the audit actor is the operator the token belongs to, not a header the
 * caller chose. The legacy single shared token is accepted as operator {@code admin} when no file is configured.
 */
public final class OperatorTokens
{
    private final Map<String, String> nameByHash;

    private OperatorTokens(final Map<String, String> nameByHash)
    {
        this.nameByHash = nameByHash;
    }

    /** One shared token, operator name {@code admin}. */
    public static OperatorTokens single(final String token)
    {
        final Map<String, String> m = new LinkedHashMap<>();
        m.put(AccessDirectory.sha256Hex(token), "admin");
        return new OperatorTokens(m);
    }

    public static OperatorTokens load(final Path file)
    {
        final Map<String, String> m = new LinkedHashMap<>();
        try
        {
            for (final String raw : Files.readAllLines(file, StandardCharsets.UTF_8))
            {
                final String line = raw.strip();
                if (line.isEmpty() || line.startsWith("#"))
                {
                    continue;
                }
                final String[] p = line.split("\\s+");
                if (p.length != 2 || !p[0].matches("[A-Za-z0-9_.@-]{1,64}") || !p[1].matches("[0-9a-f]{64}"))
                {
                    throw new IllegalArgumentException("bad operator line in " + file + ": expected '<name> <sha256 hex>'");
                }
                if (m.put(p[1], p[0]) != null)
                {
                    throw new IllegalArgumentException("two operators share a token in " + file);
                }
            }
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("cannot read operators file " + file, e);
        }
        if (m.isEmpty())
        {
            throw new IllegalArgumentException("operators file " + file + " lists no operator");
        }
        return new OperatorTokens(m);
    }

    /** The operator a presented token belongs to, or null. */
    public String operator(final String token)
    {
        if (token == null || token.isEmpty())
        {
            return null;
        }
        final byte[] h = AccessDirectory.sha256Hex(token).getBytes(StandardCharsets.US_ASCII);
        String found = null;
        for (final Map.Entry<String, String> e : nameByHash.entrySet())
        {
            if (MessageDigest.isEqual(h, e.getKey().getBytes(StandardCharsets.US_ASCII)))
            {
                found = e.getValue();
            }
        }
        return found;
    }

    public int size()
    {
        return nameByHash.size();
    }
}
