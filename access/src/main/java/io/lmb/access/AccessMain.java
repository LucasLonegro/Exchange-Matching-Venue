package io.lmb.access;

import io.lmb.common.Config;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/** The access service process. Prints {@code LMB_READY access <instance>} once both listeners are up. */
public final class AccessMain
{
    private static final Logger LOG = LoggerFactory.getLogger(AccessMain.class);

    private AccessMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final int instance = Config.getInt("lmb.instance.id", 0);
        final AccessConfig cfg = AccessConfig.load(Topology.load(), instance);
        final AccessService service = new AccessService(cfg).start();
        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("access {} shutting down", instance);
            service.close();
            stopped.countDown();
        }, "access-shutdown"));
        System.out.println("LMB_READY access " + instance);
        System.out.flush();
        stopped.await();
    }
}
