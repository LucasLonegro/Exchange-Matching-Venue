package io.lmb.replayer;

import io.lmb.api.ShardStateMachineFactory;

import java.io.File;

/**
 * Command line offline verifier:
 * {@code VerifyMain <memberDir> [--to <logPosition>] [--from-snapshot] [--shard <id>]}.
 * Prints the report; exit status 0 = verified, 2 = divergence, 1 = error.
 */
public final class VerifyMain
{
    private VerifyMain()
    {
    }

    public static void main(final String[] args)
    {
        System.exit(run(args));
    }

    static int run(final String[] args)
    {
        File dir = null;
        long to = -1;
        boolean fromSnapshot = false;
        int shard = -1;
        try
        {
            for (int i = 0; i < args.length; i++)
            {
                switch (args[i])
                {
                    case "--to" -> to = Long.parseLong(args[++i]);
                    case "--from-snapshot" -> fromSnapshot = true;
                    case "--shard" -> shard = Integer.parseInt(args[++i]);
                    default ->
                    {
                        if (args[i].startsWith("--") || dir != null)
                        {
                            throw new IllegalArgumentException("unexpected argument " + args[i]);
                        }
                        dir = new File(args[i]);
                    }
                }
            }
            if (dir == null)
            {
                throw new IllegalArgumentException("usage: VerifyMain <memberDir> [--to <logPosition>] [--from-snapshot] [--shard <id>]");
            }
            final OfflineVerifier.Report report = OfflineVerifier.verify(
                dir, ShardStateMachineFactory.load(), new OfflineVerifier.Options(to, fromSnapshot, shard));
            System.out.println(report);
            return report.ok() ? 0 : 2;
        }
        catch (final RuntimeException e)
        {
            System.err.println("verify failed: " + e.getMessage());
            return 1;
        }
    }
}
