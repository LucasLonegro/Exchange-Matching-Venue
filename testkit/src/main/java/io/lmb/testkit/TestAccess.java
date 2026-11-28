package io.lmb.testkit;

import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.Scope;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Access directories for tests, built in memory the way the access service would publish them. Production
 * credentials only ever come from the access service.
 */
public final class TestAccess
{
    /** A firm with one firm-wide key holding every scope, and its sessions. */
    public record TestFirm(int firmId, String name, String apiKey, long... sessionIds)
    {
    }

    private TestAccess()
    {
    }

    public static TestFirm firm(final int firmId, final String name, final String apiKey, final long... sessionIds)
    {
        return new TestFirm(firmId, name, apiKey, sessionIds);
    }

    /** Every firm in {@link AccessDirectory#DEFAULT_TIER}; no tiers, so the consumer's configured limits apply. */
    public static AccessDirectory directory(final TestFirm... firms)
    {
        final List<AccessDirectory.FirmEntry> fs = new ArrayList<>();
        final List<AccessDirectory.SessionEntry> ss = new ArrayList<>();
        final List<AccessDirectory.KeyEntry> ks = new ArrayList<>();
        for (final TestFirm f : firms)
        {
            fs.add(new AccessDirectory.FirmEntry(f.firmId(), f.name(), AccessDirectory.DEFAULT_TIER, true));
            for (final long s : f.sessionIds())
            {
                ss.add(new AccessDirectory.SessionEntry(s, f.firmId(), "session " + s, null, true));
            }
            ks.add(new AccessDirectory.KeyEntry("test-" + f.firmId(), AccessDirectory.sha256Hex(f.apiKey()), f.firmId(), 0,
                EnumSet.allOf(Scope.class), 0));
        }
        return new AccessDirectory(1, 0, List.of(), fs, ss, ks);
    }
}
