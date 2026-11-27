package io.lmb.replayer;

import io.lmb.sbe.MessageHeaderEncoder;
import org.agrona.MutableDirectBuffer;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Encodes a command written as text into its SBE bytes (messageHeader + body), using the generated
 * {@code io.lmb.sbe.<Message>Encoder} classes:
 *
 * <pre>
 * NewOrder sessionId=1 clOrdId=7 firmId=1 accountId=1 symbolId=1 side=BUY ordType=LIMIT timeInForce=DAY price=10000 quantity=100
 * SymbolStateCommand correlationId=3 symbolId=null targetState=HALTED
 * SymbolDefinition correlationId=1 symbolId=1 ticker=ACME tickSize=1 lotSize=1 referencePrice=10000 maxOrderQty=100000 priceBandBps=1000 initialState=CONTINUOUS
 * SymbolDefinition correlationId=2 symbolId=2 ticker=BETA tickSize=1 lotSize=1 referencePrice=5000 maxOrderQty=100000 priceBandBps=1000 dynamicBandBps=100 volatilityAuctionNs=300000000000 initialState=CONTINUOUS
 * </pre>
 *
 * The first token is the message name from schemas/venue.xml; every other token is {@code field=value} where the
 * value is an integer, an enum constant, a string (ticker) or {@code null} (the field's SBE null value). Fields
 * not given are 0 (enums: their first value is NOT implied; give them). Used by the backtester's command files
 * and by tests. Not for the hot path (reflection, allocation).
 */
public final class CommandText
{
    private final Map<String, Object> encoders = new HashMap<>();
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    /** Encodes {@code text} at offset 0 of {@code buffer} (zeroing the bytes it uses first); returns the length. */
    public int encode(final String text, final MutableDirectBuffer buffer)
    {
        final String[] tokens = text.trim().split("\\s+");
        final Object encoder = encoder(tokens[0]);
        try
        {
            final Class<?> type = encoder.getClass();
            final int blockLength = (Integer)type.getField("BLOCK_LENGTH").get(null);
            buffer.setMemory(0, MessageHeaderEncoder.ENCODED_LENGTH + blockLength, (byte)0);
            type.getMethod("wrapAndApplyHeader", MutableDirectBuffer.class, int.class, MessageHeaderEncoder.class)
                .invoke(encoder, buffer, 0, header);
            for (int i = 1; i < tokens.length; i++)
            {
                final int eq = tokens[i].indexOf('=');
                if (eq <= 0)
                {
                    throw new IllegalArgumentException("expected field=value, got '" + tokens[i] + "'");
                }
                set(encoder, tokens[i].substring(0, eq), tokens[i].substring(eq + 1));
            }
            return MessageHeaderEncoder.ENCODED_LENGTH + (Integer)type.getMethod("encodedLength").invoke(encoder);
        }
        catch (final ReflectiveOperationException e)
        {
            throw new IllegalArgumentException("cannot encode '" + text + "': " + e, e);
        }
    }

    private Object encoder(final String message)
    {
        return encoders.computeIfAbsent(message, m ->
        {
            try
            {
                return Class.forName("io.lmb.sbe." + m + "Encoder").getConstructor().newInstance();
            }
            catch (final ReflectiveOperationException e)
            {
                throw new IllegalArgumentException("unknown command '" + m + "'");
            }
        });
    }

    private static void set(final Object encoder, final String field, final String value) throws ReflectiveOperationException
    {
        Method setter = null;
        for (final Method m : encoder.getClass().getMethods())
        {
            if (m.getName().equals(field) && m.getParameterCount() == 1 && !Modifier.isStatic(m.getModifiers()) &&
                (setter == null || m.getParameterTypes()[0] == String.class))
            {
                setter = m;
            }
        }
        if (setter == null)
        {
            throw new IllegalArgumentException("no field '" + field + "' in " + encoder.getClass().getSimpleName());
        }
        final Class<?> p = setter.getParameterTypes()[0];
        final Object arg;
        if ("null".equals(value) && p.isPrimitive())
        {
            arg = encoder.getClass().getMethod(field + "NullValue").invoke(null);
        }
        else if (p == long.class)
        {
            arg = Long.parseLong(value);
        }
        else if (p == int.class)
        {
            arg = Integer.parseInt(value);
        }
        else if (p == short.class)
        {
            arg = Short.parseShort(value);
        }
        else if (p == byte.class)
        {
            arg = Byte.parseByte(value);
        }
        else if (p.isEnum())
        {
            arg = p.getMethod("valueOf", String.class).invoke(null, value);
        }
        else
        {
            arg = value;
        }
        setter.invoke(encoder, arg instanceof Number n && p == int.class ? (Object)n.intValue() : arg);
    }
}
