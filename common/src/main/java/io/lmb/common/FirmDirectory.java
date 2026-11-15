package io.lmb.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static participant data. CSV header:
 * {@code firmId,name,apiKey,sessionIds,creditLimit,maxPositionPerSymbol}
 * where sessionIds is a ';'-separated list and creditLimit is the firm-wide limit in cents
 * (the risk-budget allocator splits it across shards).
 */
public final class FirmDirectory
{
    public record Firm(
        int firmId, String name, String apiKey, List<Long> sessionIds, long creditLimit, long maxPositionPerSymbol)
    {
    }

    private final List<Firm> firms;
    private final Map<Integer, Firm> byId = new HashMap<>();
    private final Map<Long, Firm> bySession = new HashMap<>();

    public FirmDirectory(final List<Firm> firms)
    {
        this.firms = Collections.unmodifiableList(new ArrayList<>(firms));
        for (final Firm f : firms)
        {
            if (byId.put(f.firmId(), f) != null)
            {
                throw new IllegalArgumentException("duplicate firm: " + f.firmId());
            }
            for (final long s : f.sessionIds())
            {
                if (bySession.put(s, f) != null)
                {
                    throw new IllegalArgumentException("duplicate session: " + s);
                }
            }
        }
    }

    public static FirmDirectory load(final Path csv)
    {
        final List<Firm> out = new ArrayList<>();
        try
        {
            for (final String raw : Files.readAllLines(csv))
            {
                final String line = raw.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith("firmId"))
                {
                    continue;
                }
                final String[] f = line.split(",");
                final List<Long> sessions = new ArrayList<>();
                for (final String s : f[3].trim().split(";"))
                {
                    if (!s.isBlank())
                    {
                        sessions.add(Long.parseLong(s.trim()));
                    }
                }
                out.add(new Firm(
                    Integer.parseInt(f[0].trim()), f[1].trim(), f[2].trim(), List.copyOf(sessions),
                    Long.parseLong(f[4].trim()), Long.parseLong(f[5].trim())));
            }
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("cannot read firms file " + csv, e);
        }
        return new FirmDirectory(out);
    }

    public List<Firm> all()
    {
        return firms;
    }

    /** null if unknown. */
    public Firm byId(final int firmId)
    {
        return byId.get(firmId);
    }

    /** null if unknown. */
    public Firm bySession(final long sessionId)
    {
        return bySession.get(sessionId);
    }
}
