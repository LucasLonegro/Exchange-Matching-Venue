package io.lmb.cluster;

import io.aeron.security.Authenticator;
import io.aeron.security.SessionProxy;
import io.lmb.cluster.client.SharedSecret;
import org.agrona.collections.Long2LongHashMap;

import java.security.MessageDigest;

/**
 * Accepts cluster ingress sessions (and backup queries) whose credentials carry the shared secret
 * ({@code lmb.cluster.secret}); rejects everything else. Comparison is constant time.
 */
public final class SharedSecretAuthenticator implements Authenticator
{
    private static final long PENDING = 0;
    private static final long ACCEPT = 1;
    private static final long REJECT = 2;

    private final byte[] expected;
    private final Long2LongHashMap verdicts = new Long2LongHashMap(PENDING);

    public SharedSecretAuthenticator(final String secret)
    {
        this.expected = SharedSecret.encode(secret);
    }

    @Override
    public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
    {
        verdicts.put(sessionId, encodedCredentials != null && MessageDigest.isEqual(expected, encodedCredentials) ?
            ACCEPT : REJECT);
    }

    @Override
    public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
    {
        onConnectRequest(sessionId, encodedCredentials, nowMs);
    }

    @Override
    public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
    {
        decide(sessionProxy);
    }

    @Override
    public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
    {
        decide(sessionProxy);
    }

    private void decide(final SessionProxy proxy)
    {
        final long verdict = verdicts.get(proxy.sessionId());
        if (verdict == ACCEPT)
        {
            if (proxy.authenticate(null))
            {
                verdicts.remove(proxy.sessionId());
            }
        }
        else
        {
            proxy.reject();
            verdicts.remove(proxy.sessionId());
        }
    }
}
