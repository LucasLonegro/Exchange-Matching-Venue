package io.lmb.access;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;

/**
 * API key format: {@code lmb_<keyId>_<secret>}, keyId 12 hex characters (public: listings, logs, audit), secret 32
 * random bytes in URL-safe base64. Only the SHA-256 of the whole key is stored. A 256-bit random secret needs no slow
 * password hash: guessing it is out of reach, and the hash only has to make a leaked table useless.
 */
public final class ApiKeys
{
    private static final SecureRandom RANDOM = new SecureRandom();

    private ApiKeys()
    {
    }

    public static String newKeyId()
    {
        final byte[] b = new byte[6];
        RANDOM.nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    public static String newKey(final String keyId)
    {
        final byte[] b = new byte[32];
        RANDOM.nextBytes(b);
        return "lmb_" + keyId + "_" + Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    /** A random operator or service token (same strength as a key secret). */
    public static String newToken()
    {
        final byte[] b = new byte[32];
        RANDOM.nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }
}
