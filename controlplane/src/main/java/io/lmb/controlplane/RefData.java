package io.lmb.controlplane;

import io.lmb.controlplane.db.Database;
import io.lmb.controlplane.db.Store;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable snapshot of the database's reference data (symbols, firms, limits, calendar overrides). */
public record RefData(
    List<Store.SymbolRow> symbols, Map<Integer, Store.SymbolRow> symbolById,
    List<Store.FirmRow> firms, Map<Integer, Store.FirmRow> firmById, List<Store.CalendarRow> calendar)
{
    public static RefData load(final Database db)
    {
        return db.read(c ->
        {
            final List<Store.SymbolRow> symbols = Store.symbols(c);
            final List<Store.FirmRow> firms = Store.firms(c);
            final Map<Integer, Store.SymbolRow> s = new LinkedHashMap<>();
            symbols.forEach(r -> s.put(r.symbolId(), r));
            final Map<Integer, Store.FirmRow> f = new LinkedHashMap<>();
            firms.forEach(r -> f.put(r.firmId(), r));
            return new RefData(symbols, s, firms, f, Store.calendar(c));
        });
    }
}
