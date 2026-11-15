package io.lmb.api;

import org.agrona.DirectBuffer;

/**
 * Receives events emitted by a {@link ShardStateMachine}. Each call carries exactly one SBE message
 * (messageHeader + body, templateId >= 100) whose first body field is the EventHeader.
 * The buffer is only valid during the call.
 */
@FunctionalInterface
public interface EventSink
{
    void onEvent(DirectBuffer buffer, int offset, int length);
}
