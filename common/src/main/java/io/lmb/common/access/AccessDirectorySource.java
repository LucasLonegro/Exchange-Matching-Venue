package io.lmb.common.access;

import io.lmb.common.PolledSnapshot;

import java.nio.file.Path;
import java.util.List;

/**
 * Where a gateway (or the drop copy) gets its {@link AccessDirectory}: a fixed snapshot (tests) or the access
 * service's {@code GET /internal/v1/directory}, polled with {@code If-None-Match: <version>} (see
 * {@link PolledSnapshot}: the last snapshot stays in force while the service is down, and survives restarts in the
 * cache file).
 */
public final class AccessDirectorySource extends PolledSnapshot<AccessDirectory>
{
    private static final Codec<AccessDirectory> CODEC = new Codec<>()
    {
        @Override
        public AccessDirectory parse(final String json)
        {
            return AccessDirectory.fromJson(json);
        }

        @Override
        public String version(final AccessDirectory d)
        {
            return Long.toString(d.version());
        }

        @Override
        public String describe(final AccessDirectory d)
        {
            return "version " + d.version() + ": " + d.firms().size() + " firms, " + d.sessions().size() + " sessions, " +
                d.keys().size() + " keys";
        }
    };

    private AccessDirectorySource(final AccessDirectory fixed)
    {
        super("access directory", CODEC, fixed);
    }

    private AccessDirectorySource(final String url, final String token, final long pollMs, final Path cacheFile, final String name)
    {
        super("access directory", CODEC, List.of(url), "/internal/v1/directory", token, pollMs, cacheFile,
            "access-directory-" + name);
    }

    public static AccessDirectorySource fixed(final AccessDirectory directory)
    {
        return new AccessDirectorySource(directory);
    }

    /** Polls {@code url + "/internal/v1/directory"}; call {@link #start()}. */
    public static AccessDirectorySource polling(
        final String url, final String token, final long pollMs, final Path cacheFile, final String name)
    {
        return new AccessDirectorySource(url, token, pollMs, cacheFile, name);
    }

    @Override
    public AccessDirectorySource start()
    {
        super.start();
        return this;
    }
}
