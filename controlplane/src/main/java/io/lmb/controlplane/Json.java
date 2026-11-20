package io.lmb.controlplane;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tiny JSON writer and parser (maps, lists, strings, numbers, booleans, null). Avoids a Jackson dependency. */
public final class Json
{
    private Json()
    {
    }

    public static String write(final Object value)
    {
        final StringBuilder sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    private static void write(final StringBuilder sb, final Object v)
    {
        if (v == null)
        {
            sb.append("null");
        }
        else if (v instanceof String s)
        {
            quote(sb, s);
        }
        else if (v instanceof Number || v instanceof Boolean)
        {
            sb.append(v);
        }
        else if (v instanceof Map<?, ?> m)
        {
            sb.append('{');
            boolean first = true;
            for (final Map.Entry<?, ?> e : m.entrySet())
            {
                if (!first)
                {
                    sb.append(',');
                }
                first = false;
                quote(sb, String.valueOf(e.getKey()));
                sb.append(':');
                write(sb, e.getValue());
            }
            sb.append('}');
        }
        else if (v instanceof Collection<?> c)
        {
            sb.append('[');
            boolean first = true;
            for (final Object o : c)
            {
                if (!first)
                {
                    sb.append(',');
                }
                first = false;
                write(sb, o);
            }
            sb.append(']');
        }
        else
        {
            quote(sb, v.toString());
        }
    }

    private static void quote(final StringBuilder sb, final String s)
    {
        sb.append('"');
        for (int i = 0; i < s.length(); i++)
        {
            final char ch = s.charAt(i);
            switch (ch)
            {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default ->
                {
                    if (ch < 0x20)
                    {
                        sb.append(String.format("\\u%04x", (int)ch));
                    }
                    else
                    {
                        sb.append(ch);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** Parses a JSON document. Numbers become Long when integral, else Double. */
    public static Object parse(final String text)
    {
        final Parser p = new Parser(text);
        p.ws();
        final Object v = p.value();
        p.ws();
        if (p.i != text.length())
        {
            throw new IllegalArgumentException("trailing characters at " + p.i);
        }
        return v;
    }

    /** Parses a JSON object (empty or blank text = empty object). */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(final String text)
    {
        if (text == null || text.isBlank())
        {
            return new LinkedHashMap<>();
        }
        final Object v = parse(text);
        if (!(v instanceof Map))
        {
            throw new IllegalArgumentException("expected a JSON object");
        }
        return (Map<String, Object>)v;
    }

    private static final class Parser
    {
        private final String s;
        private int i;

        Parser(final String s)
        {
            this.s = s;
        }

        void ws()
        {
            while (i < s.length() && Character.isWhitespace(s.charAt(i)))
            {
                i++;
            }
        }

        Object value()
        {
            if (i >= s.length())
            {
                throw new IllegalArgumentException("unexpected end of JSON");
            }
            final char c = s.charAt(i);
            if (c == '{')
            {
                return object();
            }
            if (c == '[')
            {
                return array();
            }
            if (c == '"')
            {
                return string();
            }
            if (s.startsWith("true", i))
            {
                i += 4;
                return Boolean.TRUE;
            }
            if (s.startsWith("false", i))
            {
                i += 5;
                return Boolean.FALSE;
            }
            if (s.startsWith("null", i))
            {
                i += 4;
                return null;
            }
            return number();
        }

        Map<String, Object> object()
        {
            final Map<String, Object> m = new LinkedHashMap<>();
            i++;
            ws();
            if (peek() == '}')
            {
                i++;
                return m;
            }
            while (true)
            {
                ws();
                final String k = string();
                ws();
                expect(':');
                ws();
                m.put(k, value());
                ws();
                if (peek() == ',')
                {
                    i++;
                    continue;
                }
                expect('}');
                return m;
            }
        }

        List<Object> array()
        {
            final List<Object> l = new ArrayList<>();
            i++;
            ws();
            if (peek() == ']')
            {
                i++;
                return l;
            }
            while (true)
            {
                ws();
                l.add(value());
                ws();
                if (peek() == ',')
                {
                    i++;
                    continue;
                }
                expect(']');
                return l;
            }
        }

        String string()
        {
            expect('"');
            final StringBuilder sb = new StringBuilder();
            while (true)
            {
                if (i >= s.length())
                {
                    throw new IllegalArgumentException("unterminated string");
                }
                final char c = s.charAt(i++);
                if (c == '"')
                {
                    return sb.toString();
                }
                if (c == '\\')
                {
                    final char e = s.charAt(i++);
                    switch (e)
                    {
                        case 'n' -> sb.append('\n');
                        case 't' -> sb.append('\t');
                        case 'r' -> sb.append('\r');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'u' ->
                        {
                            sb.append((char)Integer.parseInt(s.substring(i, i + 4), 16));
                            i += 4;
                        }
                        default -> sb.append(e);
                    }
                }
                else
                {
                    sb.append(c);
                }
            }
        }

        Object number()
        {
            final int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0)
            {
                i++;
            }
            final String n = s.substring(start, i);
            if (n.isEmpty())
            {
                throw new IllegalArgumentException("unexpected character at " + start);
            }
            if (n.contains(".") || n.contains("e") || n.contains("E"))
            {
                return Double.parseDouble(n);
            }
            return Long.parseLong(n);
        }

        char peek()
        {
            if (i >= s.length())
            {
                throw new IllegalArgumentException("unexpected end of JSON");
            }
            return s.charAt(i);
        }

        void expect(final char c)
        {
            if (peek() != c)
            {
                throw new IllegalArgumentException("expected '" + c + "' at " + i);
            }
            i++;
        }
    }
}
