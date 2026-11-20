package io.lmb.controlplane;

import io.lmb.controlplane.db.Store;
import io.lmb.sbe.SymbolState;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The trading calendar: which dates trade (weekdays minus database overrides) and, per trading date, the
 * wall-clock times (in one zone) of the phases PRE_OPEN, OPEN_AUCTION, CONTINUOUS, CLOSE_AUCTION, CLOSE and
 * DAY_ROLL. DAY_ROLL sends DayRollover for the next trading date (which leaves every symbol PRE_OPEN).
 * A phase time of "-" (in configuration or an override) means the phase does not happen that day.
 */
public final class TradingCalendar
{
    public enum Phase
    {
        PRE_OPEN(SymbolState.PRE_OPEN),
        OPEN_AUCTION(SymbolState.AUCTION_CALL),
        CONTINUOUS(SymbolState.CONTINUOUS),
        CLOSE_AUCTION(SymbolState.AUCTION_CALL),
        CLOSE(SymbolState.CLOSED),
        DAY_ROLL(null);

        public final SymbolState target;

        Phase(final SymbolState target)
        {
            this.target = target;
        }
    }

    public record Transition(int tradingDate, Phase phase, long epochNs, int rollToDate)
    {
        /** State the transition leads to (DAY_ROLL: PRE_OPEN via the rollover). */
        public SymbolState state()
        {
            return phase == Phase.DAY_ROLL ? SymbolState.PRE_OPEN : phase.target;
        }
    }

    private static final long DAY_NS = 86_400_000_000_000L;

    private final ZoneId zone;
    private final Set<DayOfWeek> weekdays;
    private final Map<Phase, String> defaults;
    private final Map<Integer, Store.CalendarRow> overrides = new HashMap<>();

    public TradingCalendar(final ZoneId zone, final Set<DayOfWeek> weekdays, final Map<Phase, String> defaults,
        final List<Store.CalendarRow> overrides)
    {
        this.zone = zone;
        this.weekdays = weekdays;
        this.defaults = defaults;
        overrides.forEach(r -> this.overrides.put(r.tradingDate(), r));
    }

    public static TradingCalendar from(final ControlPlaneConfig cfg, final List<Store.CalendarRow> overrides)
    {
        final Set<DayOfWeek> days = EnumSet.noneOf(DayOfWeek.class);
        for (final String d : cfg.calendarWeekdays.split(","))
        {
            if (!d.isBlank())
            {
                days.add(parseDay(d.trim()));
            }
        }
        final Map<Phase, String> times = new EnumMap<>(Phase.class);
        times.put(Phase.PRE_OPEN, cfg.calPreOpen);
        times.put(Phase.OPEN_AUCTION, cfg.calOpenAuction);
        times.put(Phase.CONTINUOUS, cfg.calContinuous);
        times.put(Phase.CLOSE_AUCTION, cfg.calCloseAuction);
        times.put(Phase.CLOSE, cfg.calClose);
        times.put(Phase.DAY_ROLL, cfg.calDayRoll);
        return new TradingCalendar(ZoneId.of(cfg.calendarZone), days, times, overrides);
    }

    private static DayOfWeek parseDay(final String d)
    {
        for (final DayOfWeek w : DayOfWeek.values())
        {
            if (w.name().startsWith(d.toUpperCase()))
            {
                return w;
            }
        }
        throw new IllegalArgumentException("unknown weekday " + d);
    }

    public ZoneId zone()
    {
        return zone;
    }

    public static int yyyymmdd(final LocalDate d)
    {
        return d.getYear() * 10_000 + d.getMonthValue() * 100 + d.getDayOfMonth();
    }

    public static LocalDate date(final int yyyymmdd)
    {
        return LocalDate.parse(Integer.toString(yyyymmdd), DateTimeFormatter.BASIC_ISO_DATE);
    }

    public LocalDate localDate(final long epochNs)
    {
        return Instant.ofEpochSecond(0, epochNs).atZone(zone).toLocalDate();
    }

    public boolean isTradingDay(final LocalDate d)
    {
        final Store.CalendarRow o = overrides.get(yyyymmdd(d));
        return o != null ? o.tradingDay() : weekdays.contains(d.getDayOfWeek());
    }

    public LocalDate nextTradingDay(final LocalDate d)
    {
        LocalDate x = d.plusDays(1);
        for (int i = 0; i < 400 && !isTradingDay(x); i++)
        {
            x = x.plusDays(1);
        }
        return x;
    }

    /** The transitions of one date, in time order (empty for a non-trading date). */
    public List<Transition> day(final LocalDate d)
    {
        final List<Transition> out = new ArrayList<>();
        if (!isTradingDay(d))
        {
            return out;
        }
        final Store.CalendarRow o = overrides.get(yyyymmdd(d));
        long last = Long.MIN_VALUE;
        for (final Phase p : Phase.values())
        {
            String t = o == null ? null : switch (p)
            {
                case PRE_OPEN -> o.preOpen();
                case OPEN_AUCTION -> o.openAuction();
                case CONTINUOUS -> o.continuous();
                case CLOSE_AUCTION -> o.closeAuction();
                case CLOSE -> o.closeTime();
                case DAY_ROLL -> o.dayRoll();
            };
            if (t == null)
            {
                t = defaults.get(p);
            }
            if (t == null || t.isBlank() || t.equals("-"))
            {
                continue;
            }
            final long ns = epochNs(d, LocalTime.parse(t));
            if (ns <= last)
            {
                throw new IllegalStateException("calendar phases out of order on " + d + " at " + p);
            }
            last = ns;
            out.add(new Transition(yyyymmdd(d), p, ns, p == Phase.DAY_ROLL ? yyyymmdd(nextTradingDay(d)) : 0));
        }
        return out;
    }

    private long epochNs(final LocalDate d, final LocalTime t)
    {
        final Instant i = d.atTime(t).atZone(zone).toInstant();
        return i.getEpochSecond() * 1_000_000_000L + i.getNano();
    }

    /** Transitions with fromNs < time <= toNs, in time order. */
    public List<Transition> between(final long fromNs, final long toNs)
    {
        final List<Transition> out = new ArrayList<>();
        for (LocalDate d = localDate(fromNs).minusDays(1); !d.isAfter(localDate(toNs).plusDays(1)); d = d.plusDays(1))
        {
            for (final Transition t : day(d))
            {
                if (t.epochNs() > fromNs && t.epochNs() <= toNs)
                {
                    out.add(t);
                }
            }
        }
        return out;
    }

    /** The latest transition at or before the instant (looking back up to two weeks), or null. */
    public Transition lastAtOrBefore(final long epochNs)
    {
        final List<Transition> l = between(epochNs - 14 * DAY_NS, epochNs);
        return l.isEmpty() ? null : l.get(l.size() - 1);
    }
}
