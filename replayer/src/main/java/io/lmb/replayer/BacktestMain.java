package io.lmb.replayer;

import io.lmb.api.ShardStateMachineFactory;

import java.io.File;

/**
 * Command line backtester:
 * {@code BacktestMain --out <dir> [--log <memberDir>] [--to <logPosition>] [--from-snapshot] [--commands <file>]
 * [--shard <id>] [--step-ns <ns>] [--drain-timers]}. Writes events.bin, fills.csv and summary.txt into
 * {@code --out} (see {@link Backtest}) and prints the summary. Exit status 0 = done, 1 = error.
 */
public final class BacktestMain
{
    static final String USAGE = "usage: BacktestMain --out <dir> [--log <memberDir>] [--to <logPosition>] [--from-snapshot] " +
        "[--commands <file>] [--shard <id>] [--step-ns <ns>] [--drain-timers]";

    private BacktestMain()
    {
    }

    public static void main(final String[] args)
    {
        System.exit(run(args));
    }

    static int run(final String[] args)
    {
        File out = null;
        File log = null;
        File commands = null;
        long to = -1;
        boolean fromSnapshot = false;
        int shard = -1;
        long step = 1_000;
        boolean drain = false;
        try
        {
            for (int i = 0; i < args.length; i++)
            {
                switch (args[i])
                {
                    case "--out" -> out = new File(value(args, ++i));
                    case "--log" -> log = new File(value(args, ++i));
                    case "--commands" -> commands = new File(value(args, ++i));
                    case "--to" -> to = Long.parseLong(value(args, ++i));
                    case "--from-snapshot" -> fromSnapshot = true;
                    case "--shard" -> shard = Integer.parseInt(value(args, ++i));
                    case "--step-ns" -> step = Long.parseLong(value(args, ++i));
                    case "--drain-timers" -> drain = true;
                    default -> throw new IllegalArgumentException("unexpected argument " + args[i] + "\n" + USAGE);
                }
            }
            if (out == null || (log == null && commands == null))
            {
                throw new IllegalArgumentException(USAGE);
            }
            Backtest.run(ShardStateMachineFactory.load(),
                new Backtest.Options(log, to, fromSnapshot, commands, shard, step, drain, out));
            System.out.print(new String(java.nio.file.Files.readAllBytes(new File(out, "summary.txt").toPath()),
                java.nio.charset.StandardCharsets.UTF_8));
            return 0;
        }
        catch (final RuntimeException | java.io.IOException e)
        {
            System.err.println("backtest failed: " + e.getMessage());
            return 1;
        }
    }

    private static String value(final String[] args, final int i)
    {
        if (i >= args.length)
        {
            throw new IllegalArgumentException("missing value for " + args[i - 1]);
        }
        return args[i];
    }
}
