package io.lmb.matcher;

import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;

/** ServiceLoader entry point: {@code ShardStateMachineFactory.load().create(shardId)}. */
public final class MatcherShardFactory implements ShardStateMachineFactory
{
    @Override
    public ShardStateMachine create(final int shardId)
    {
        return new MatcherShard(shardId, MatcherConfig.fromConfig());
    }
}
