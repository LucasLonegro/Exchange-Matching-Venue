package io.lmb.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A JSON snapshot served by another service and polled on a daemon thread with {@code If-None-Match: "<version>"}:
 * the access directory (access service) and the symbol directory (control plane). Either fixed (tests) or polled.
 *
 * <p>Neither source is on the trading path. While every URL is unreachable the last snapshot stays in force, and
 * each new snapshot is written to {@code cacheFile}, so a consumer restarted during an outage starts from what it
 * last saw. {@link #current()} is null only before the first snapshot ever (no cache, no answer). With several
 * base URLs (both control plane instances) a poll tries them in turn, starting with the last one that answered.
 */
public class PolledSnapshot<T> implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(PolledSnapshot.class);

    /** How a snapshot is parsed, versioned and logged. */
    public interface Codec<T>
    {
        T parse(String json);

        /** Compared with the server's ETag; equal versions mean an unchanged snapshot. */
        String version(T value);

        /** One line for the log. */
        String describe(T value);
    }

    private volatile T current;
    /** Wall-clock ms of the last successful poll (200 or 304), or of loading a fixed snapshot. */
    public final AtomicLong lastSuccessMs = new AtomicLong();
    public final AtomicLong pollFailures = new AtomicLong();
    /** Incremented on every new snapshot, so a consumer can tell cheaply that it changed. */
    public final AtomicLong updates = new AtomicLong();

    private final String what;
    private final Codec<T> codec;
    private final List<String> urls;
    private final String token;
    private final long pollMs;
    private final Path cacheFile;
    private final HttpClient http;
    private final Thread thread;
    private volatile int preferred;
    private volatile boolean closed;

    protected PolledSnapshot(final String what, final Codec<T> codec, final T fixed)
    {
        this.what = what;
        this.codec = codec;
        this.current = fixed;
        this.lastSuccessMs.set(System.currentTimeMillis());
        this.urls = List.of();
        this.token = null;
        this.pollMs = 0;
        this.cacheFile = null;
        this.http = null;
        this.thread = null;
    }

    /**
     * @param baseUrls tried in turn; {@code path} is appended to each
     * @param path     e.g. {@code /internal/v1/directory}
     */
    protected PolledSnapshot(final String what, final Codec<T> codec, final List<String> baseUrls, final String path,
        final String token, final long pollMs, final Path cacheFile, final String threadName)
    {
        if (baseUrls.isEmpty())
        {
            throw new IllegalArgumentException(what + ": no URL to poll");
        }
        this.what = what;
        this.codec = codec;
        final List<String> u = new ArrayList<>();
        for (final String b : baseUrls)
        {
            u.add((b.endsWith("/") ? b.substring(0, b.length() - 1) : b) + path);
        }
        this.urls = List.copyOf(u);
        this.token = token;
        this.pollMs = Math.max(100, pollMs);
        this.cacheFile = cacheFile;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        if (cacheFile != null && Files.exists(cacheFile))
        {
            try
            {
                current = codec.parse(Files.readString(cacheFile));
                LOG.info("{} {} loaded from cache {}", what, codec.describe(current), cacheFile);
            }
            catch (final IOException | RuntimeException e)
            {
                LOG.warn("ignoring unreadable {} cache {}: {}", what, cacheFile, e.toString());
            }
        }
        this.thread = new Thread(this::run, threadName);
        this.thread.setDaemon(true);
    }

    /** Polls once, synchronously, then starts the polling thread (fixed sources: no-op). */
    public PolledSnapshot<T> start()
    {
        if (thread != null)
        {
            pollOnce();
            thread.start();
        }
        return this;
    }

    /** The latest snapshot, or null before the first one. */
    public T current()
    {
        return current;
    }

    /** Replaces the snapshot (fixed sources: tests and reloads). */
    public void set(final T value)
    {
        current = value;
        updates.incrementAndGet();
        lastSuccessMs.set(System.currentTimeMillis());
    }

    public boolean isPolling()
    {
        return thread != null;
    }

    /** Blocks until a first snapshot exists (logging every 5 s); false if interrupted or closed first. */
    public boolean awaitFirst(final String waiter)
    {
        long nextLogMs = 0;
        while (current == null && !closed)
        {
            final long now = System.currentTimeMillis();
            if (now >= nextLogMs)
            {
                LOG.info("{} waiting for a first {} from {}", waiter, what, urls);
                nextLogMs = now + 5_000;
            }
            try
            {
                Thread.sleep(Math.min(pollMs, 500));
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return current != null;
    }

    private void run()
    {
        while (!closed)
        {
            try
            {
                Thread.sleep(pollMs);
            }
            catch (final InterruptedException e)
            {
                return;
            }
            pollOnce();
        }
    }

    /** One poll (every URL in turn until one answers); never throws. */
    public void pollOnce()
    {
        if (http == null)
        {
            return;
        }
        Exception last = null;
        final int first = preferred;
        for (int i = 0; i < urls.size(); i++)
        {
            final int idx = (first + i) % urls.size();
            try
            {
                pollUrl(urls.get(idx));
                preferred = idx;
                return;
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return;
            }
            catch (final IOException | RuntimeException e)
            {
                last = e;
            }
        }
        if (pollFailures.incrementAndGet() % 30 == 1)
        {
            final T cur = current;
            LOG.warn("{} poll of {} failed ({} so far), keeping {}: {}", what, urls, pollFailures.get(),
                cur == null ? "none" : codec.describe(cur), String.valueOf(last));
        }
    }

    private void pollUrl(final String url) throws IOException, InterruptedException
    {
        final T cur = current;
        final HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET();
        if (token != null && !token.isEmpty())
        {
            rb.header("Authorization", "Bearer " + token);
        }
        if (cur != null)
        {
            rb.header("If-None-Match", "\"" + codec.version(cur) + "\"");
        }
        final HttpResponse<String> r = http.send(rb.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (r.statusCode() == 304)
        {
            lastSuccessMs.set(System.currentTimeMillis());
            return;
        }
        if (r.statusCode() != 200)
        {
            throw new IOException("HTTP " + r.statusCode() + " from " + url);
        }
        final T next = codec.parse(r.body());
        lastSuccessMs.set(System.currentTimeMillis());
        if (cur == null || !codec.version(next).equals(codec.version(cur)))
        {
            current = next;
            updates.incrementAndGet();
            LOG.info("{} {}", what, codec.describe(next));
            writeCache(r.body());
        }
    }

    private void writeCache(final String json)
    {
        if (cacheFile == null)
        {
            return;
        }
        try
        {
            Files.createDirectories(cacheFile.toAbsolutePath().getParent());
            final Path tmp = cacheFile.resolveSibling(cacheFile.getFileName() + ".tmp");
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, cacheFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (final IOException e)
        {
            LOG.warn("cannot write {} cache {}: {}", what, cacheFile, e.toString());
        }
    }

    @Override
    public void close()
    {
        closed = true;
        if (thread != null)
        {
            thread.interrupt();
        }
    }
}
