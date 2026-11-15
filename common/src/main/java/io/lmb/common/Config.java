package io.lmb.common;

/**
 * Process configuration lookup. Order: system property, then environment variable
 * (lmb.foo.bar -> LMB_FOO_BAR), then the default.
 */
public final class Config
{
    private Config()
    {
    }

    public static String get(final String key, final String defaultValue)
    {
        final String sys = System.getProperty(key);
        if (sys != null)
        {
            return sys;
        }
        final String env = System.getenv(envName(key));
        return env != null ? env : defaultValue;
    }

    public static String require(final String key)
    {
        final String value = get(key, null);
        if (value == null)
        {
            throw new IllegalStateException("missing configuration: -D" + key + " or " + envName(key));
        }
        return value;
    }

    public static int getInt(final String key, final int defaultValue)
    {
        final String v = get(key, null);
        return v == null ? defaultValue : Integer.parseInt(v.trim());
    }

    public static long getLong(final String key, final long defaultValue)
    {
        final String v = get(key, null);
        return v == null ? defaultValue : Long.parseLong(v.trim());
    }

    public static boolean getBool(final String key, final boolean defaultValue)
    {
        final String v = get(key, null);
        return v == null ? defaultValue : Boolean.parseBoolean(v.trim());
    }

    public static String envName(final String key)
    {
        return key.toUpperCase().replace('.', '_').replace('-', '_');
    }
}
