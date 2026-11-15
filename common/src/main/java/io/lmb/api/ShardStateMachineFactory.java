package io.lmb.api;

import java.util.ServiceLoader;

/** Creates the state machine for a shard. Discovered with {@link ServiceLoader}; matcher-core provides the production one. */
public interface ShardStateMachineFactory
{
    ShardStateMachine create(int shardId);

    static ShardStateMachineFactory load()
    {
        return ServiceLoader.load(ShardStateMachineFactory.class).findFirst().orElseThrow(
            () -> new IllegalStateException("no ShardStateMachineFactory on the classpath (is lmb-matcher-core present?)"));
    }
}
