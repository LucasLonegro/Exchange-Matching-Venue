package io.lmb.posttrade.surveillance;

/**
 * Where surveillance records go. Called only on the sink's runner thread. The output owns the durable cursor:
 * {@link #open} returns, per shard, the eventSeq after which the runner must resume.
 */
public interface SurveillanceOutput
{
    long[] open(int shardCount) throws Exception;

    /** One record (a complete JSON object, ASCII). */
    void write(int shardId, long eventSeq, CharSequence json) throws Exception;

    /** An event that produces no record. */
    void skip(int shardId, long eventSeq) throws Exception;

    /** Makes everything written so far durable and moves the durable cursor. */
    void flush() throws Exception;

    /** Background duty (e.g. surfacing asynchronous send failures). */
    default int poll() throws Exception
    {
        return 0;
    }

    void close(boolean clean);
}
