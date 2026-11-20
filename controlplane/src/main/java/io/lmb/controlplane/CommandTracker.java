package io.lmb.controlplane;

import io.lmb.controlplane.TrackedCommand.Status;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Unresolved commands, indexed by correlation id and by key (the thing a command acts on, e.g.
 * {@code risk:<shard>:<firm>}). Correlation ids grow in issue order, also across leaders, so "earlier"
 * means a smaller id.
 *
 * <p>Resolution rule: when a command is confirmed, every earlier unresolved command on the same key is
 * resolved as LOST. This relies on the Raft log-prefix property: once a later offer from this client is
 * committed, an earlier offer that is not already in the log before it can never be applied.
 */
public final class CommandTracker
{
    private final TreeMap<Long, TrackedCommand> unresolved = new TreeMap<>();
    private final Map<String, TreeMap<Long, TrackedCommand>> byKey = new HashMap<>();
    private final Set<TrackedCommand> dirty = new LinkedHashSet<>();
    private final Consumer<TrackedCommand> onResolved;

    public CommandTracker(final Consumer<TrackedCommand> onResolved)
    {
        this.onResolved = onResolved;
    }

    public void add(final TrackedCommand c)
    {
        if (c.status.unresolved())
        {
            unresolved.put(c.correlationId, c);
            byKey.computeIfAbsent(c.key, k -> new TreeMap<>()).put(c.correlationId, c);
        }
        dirty.add(c);
    }

    public TrackedCommand get(final long correlationId)
    {
        return unresolved.get(correlationId);
    }

    public Collection<TrackedCommand> unresolved()
    {
        return unresolved.values();
    }

    public int size()
    {
        return unresolved.size();
    }

    /** Unresolved commands for the key, oldest first. */
    public Collection<TrackedCommand> forKey(final String key)
    {
        final TreeMap<Long, TrackedCommand> m = byKey.get(key);
        return m == null ? List.of() : m.values();
    }

    public boolean hasActive(final String key)
    {
        for (final TrackedCommand c : forKey(key))
        {
            if (c.status.active())
            {
                return true;
            }
        }
        return false;
    }

    public boolean hasActivePrefix(final String keyPrefix)
    {
        for (final TrackedCommand c : unresolved.values())
        {
            if (c.status.active() && c.key.startsWith(keyPrefix))
            {
                return true;
            }
        }
        return false;
    }

    /** Most recent unresolved command for the key, or null. */
    public TrackedCommand latest(final String key)
    {
        final TreeMap<Long, TrackedCommand> m = byKey.get(key);
        return m == null || m.isEmpty() ? null : m.lastEntry().getValue();
    }

    /** Stop retrying the active commands of this key; they stay unresolved until a later one is confirmed. */
    public void supersedeActive(final String key)
    {
        for (final TrackedCommand c : forKey(key))
        {
            if (c.status.active())
            {
                c.status = Status.SUPERSEDED;
                dirty.add(c);
            }
        }
    }

    /** Terminal outcome for c. CONFIRMED also resolves every earlier command on the same key as LOST. */
    public void resolve(final TrackedCommand c, final Status status, final String reason)
    {
        if (!c.status.unresolved())
        {
            return;
        }
        if (status == Status.CONFIRMED)
        {
            final TreeMap<Long, TrackedCommand> m = byKey.get(c.key);
            if (m != null)
            {
                for (final TrackedCommand earlier : new ArrayList<>(m.headMap(c.correlationId, false).values()))
                {
                    finish(earlier, Status.LOST, "superseded by " + c.correlationId);
                }
            }
        }
        finish(c, status, reason);
    }

    private void finish(final TrackedCommand c, final Status status, final String reason)
    {
        c.status = status;
        if (reason != null)
        {
            c.rejectReason = reason;
        }
        unresolved.remove(c.correlationId);
        final TreeMap<Long, TrackedCommand> m = byKey.get(c.key);
        if (m != null)
        {
            m.remove(c.correlationId);
            if (m.isEmpty())
            {
                byKey.remove(c.key);
            }
        }
        dirty.add(c);
        onResolved.accept(c);
    }

    public void markDirty(final TrackedCommand c)
    {
        dirty.add(c);
    }

    /** Commands whose status changed since the last call. */
    public List<TrackedCommand> drainDirty()
    {
        final List<TrackedCommand> out = new ArrayList<>(dirty);
        dirty.clear();
        return out;
    }

    public void undrain(final List<TrackedCommand> commands)
    {
        dirty.addAll(commands);
    }
}
