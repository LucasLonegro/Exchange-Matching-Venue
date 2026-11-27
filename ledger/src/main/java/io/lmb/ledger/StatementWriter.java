package io.lmb.ledger;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Daily statement files, from the database: {@code <dir>/<yyyymmdd>/summary.csv} (one line per firm, the
 * {@code ledger_statement} row) and {@code <dir>/<yyyymmdd>/firm-<firmId>.csv} (the firm's position lines, the
 * {@code ledger_daily_position} rows, with the average cost). Amounts are integer cents; an empty field is no price.
 * A date's directory is written under a temporary name and renamed into place, so it is either complete or absent;
 * files are regenerated from the database when missing, so they never need a backup of their own.
 */
final class StatementWriter
{
    static final String[] SUMMARY_COLUMNS = {"trading_date", "firm_id", "cash_flow_day", "cash_balance", "market_value",
        "equity", "realised_pnl_day", "realised_pnl_total", "unrealised_pnl", "pnl_day", "buy_notional", "sell_notional",
        "trade_count", "open_positions"};
    static final String[] POSITION_COLUMNS = {"trading_date", "firm_id", "symbol_id", "ticker", "open_qty", "bought_qty",
        "sold_qty", "close_qty", "buy_notional", "sell_notional", "cost_basis", "close_price", "close_source",
        "market_value", "unrealised_pnl", "realised_pnl_day", "realised_pnl_total", "pnl_day", "cash_flow_day",
        "cash_total", "trade_count"};
    private static final int CLOSE_QTY = 7;
    private static final int COST_BASIS = 10;

    private final Path dir;

    StatementWriter(final Path dir)
    {
        this.dir = dir;
    }

    static String dateDir(final int tradingDate)
    {
        return String.format("%08d", tradingDate);
    }

    boolean exists(final int tradingDate)
    {
        return Files.isDirectory(dir.resolve(dateDir(tradingDate)));
    }

    void write(final LedgerStore store, final int tradingDate) throws SQLException, IOException
    {
        final Path target = dir.resolve(dateDir(tradingDate));
        if (Files.isDirectory(target))
        {
            return;
        }
        final Path tmp = dir.resolve(dateDir(tradingDate) + ".tmp");
        deleteRecursively(tmp);
        Files.createDirectories(tmp);
        final List<Object[]> summary = store.statementRows(tradingDate);
        try (Writer w = Files.newBufferedWriter(tmp.resolve("summary.csv"), StandardCharsets.UTF_8))
        {
            line(w, SUMMARY_COLUMNS);
            for (final Object[] row : summary)
            {
                line(w, row);
            }
        }
        final TreeMap<Integer, Writer> perFirm = new TreeMap<>();
        try
        {
            for (final Object[] row : summary)
            {
                final int firmId = ((Number)row[1]).intValue();
                final Writer w = Files.newBufferedWriter(tmp.resolve("firm-" + firmId + ".csv"), StandardCharsets.UTF_8);
                perFirm.put(firmId, w);
                final String[] header = new String[POSITION_COLUMNS.length + 1];
                System.arraycopy(POSITION_COLUMNS, 0, header, 0, CLOSE_QTY + 1);
                header[CLOSE_QTY + 1] = "avg_cost";
                System.arraycopy(POSITION_COLUMNS, CLOSE_QTY + 1, header, CLOSE_QTY + 2,
                    POSITION_COLUMNS.length - CLOSE_QTY - 1);
                line(w, header);
            }
            for (final Object[] row : store.positionRows(tradingDate))
            {
                final Writer w = perFirm.get(((Number)row[1]).intValue());
                if (w == null)
                {
                    continue;
                }
                final Object[] out = new Object[row.length + 1];
                System.arraycopy(row, 0, out, 0, CLOSE_QTY + 1);
                out[CLOSE_QTY + 1] = averageCost(((Number)row[COST_BASIS]).longValue(), ((Number)row[CLOSE_QTY]).longValue());
                System.arraycopy(row, CLOSE_QTY + 1, out, CLOSE_QTY + 2, row.length - CLOSE_QTY - 1);
                line(w, out);
            }
        }
        finally
        {
            for (final Writer w : perFirm.values())
            {
                w.close();
            }
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
    }

    /** Cost per share in cents, 4 decimals; empty when flat. */
    static String averageCost(final long costBasis, final long quantity)
    {
        if (quantity == 0)
        {
            return "";
        }
        return BigDecimal.valueOf(costBasis).divide(BigDecimal.valueOf(quantity), 4, RoundingMode.HALF_EVEN).toPlainString();
    }

    private static void line(final Writer w, final Object[] values) throws IOException
    {
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0)
            {
                w.write(',');
            }
            final Object v = values[i];
            if (v != null)
            {
                final String s = v.toString();
                // tickers are [A-Z0-9.] in practice; quote anything that would break the line
                if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0)
                {
                    w.write('"' + s.replace("\"", "\"\"") + '"');
                }
                else
                {
                    w.write(s);
                }
            }
        }
        w.write('\n');
    }

    private static void deleteRecursively(final Path p) throws IOException
    {
        if (!Files.exists(p))
        {
            return;
        }
        try (Stream<Path> s = Files.walk(p))
        {
            for (final Path x : s.sorted(Comparator.reverseOrder()).toList())
            {
                Files.delete(x);
            }
        }
    }
}
