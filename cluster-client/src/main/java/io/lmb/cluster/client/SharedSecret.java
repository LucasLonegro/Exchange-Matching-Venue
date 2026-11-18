package io.lmb.cluster.client;

import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;

import java.nio.charset.StandardCharsets;

/** Shared-secret credentials ({@code lmb.cluster.secret}) presented on cluster ingress and backup queries. */
public final class SharedSecret
{
    private SharedSecret()
    {
    }

    public static byte[] encode(final String secret)
    {
        return ("lmb:" + secret).getBytes(StandardCharsets.UTF_8);
    }

    public static CredentialsSupplier credentials(final String secret)
    {
        if (secret == null || secret.isEmpty())
        {
            return new NullCredentialsSupplier();
        }
        final byte[] encoded = encode(secret);
        return new CredentialsSupplier()
        {
            @Override
            public byte[] encodedCredentials()
            {
                return encoded.clone();
            }

            @Override
            public byte[] onChallenge(final byte[] encodedChallenge)
            {
                return encoded.clone();
            }
        };
    }
}
