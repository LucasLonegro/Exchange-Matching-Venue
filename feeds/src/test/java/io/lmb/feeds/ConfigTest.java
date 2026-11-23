package io.lmb.feeds;

import io.lmb.common.Topology;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigTest
{
    static Topology topology(final String... keyValues)
    {
        final Properties p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2)
        {
            p.setProperty(keyValues[i], keyValues[i + 1]);
        }
        return Topology.of(p);
    }

    @Test
    void feedPortLayoutGivesEachConsolidatorItsOwnNonOverlappingRange()
    {
        final Topology t = topology("lmb.consolidators", "2");
        assertEquals(9660, FeedsConfig.feedBasePort(t, 0));
        assertEquals(9680, FeedsConfig.feedBasePort(t, 1));
        final int lastPortOfInstance0 = FeedsConfig.linePort(9660, FeedsConfig.MAX_VENUES - 1, SimFeedProtocol.LINE_B);
        assertEquals(9679, lastPortOfInstance0);
        assertEquals(9665, FeedsConfig.linePort(9660, 2, SimFeedProtocol.LINE_B));
        assertEquals(List.of(new InetSocketAddress("127.0.0.1", 9660), new InetSocketAddress("127.0.0.1", 9680)),
            FeedsConfig.simulatorTargets(t));
    }

    @Test
    void explicitSimulatorTargetsOverrideTheTopology()
    {
        final Topology t = topology(FeedsConfig.SIM_TARGETS, "127.0.0.1:35000, localhost:35100");
        assertEquals(List.of(new InetSocketAddress("127.0.0.1", 35000), new InetSocketAddress("localhost", 35100)),
            FeedsConfig.simulatorTargets(t));
        assertThrows(IllegalArgumentException.class,
            () -> FeedsConfig.simulatorTargets(topology(FeedsConfig.SIM_TARGETS, "35000")));
    }

    @Test
    void venueCountIsBounded()
    {
        assertEquals(4, FeedsConfig.venueCount(topology()));
        assertThrows(IllegalArgumentException.class, () -> FeedsConfig.venueCount(topology(FeedsConfig.VENUES, "0")));
        assertThrows(IllegalArgumentException.class, () -> FeedsConfig.venueCount(topology(FeedsConfig.VENUES, "11")));
    }

    @Test
    void consolidatorConfigDefaultsOverridesAndValidation()
    {
        final ConsolidatorConfig d = ConsolidatorConfig.from(topology(), 1);
        assertEquals(new ConsolidatorConfig(50_000_000L, 2, 10_000_000L, 1, 1, 4), d);
        final ConsolidatorConfig o = ConsolidatorConfig.from(
            topology("lmb.nbbo.staleNs", "5000000", "lmb.nbbo.minVenues", "3", "lmb.feeds.venues", "6"), 0);
        assertEquals(5_000_000L, o.staleNs());
        assertEquals(3, o.minVenues());
        assertEquals(6, o.venueCount());
        assertThrows(IllegalArgumentException.class, () -> new ConsolidatorConfig(0, 2, 1, 1, 0, 4));
        assertThrows(IllegalArgumentException.class, () -> new ConsolidatorConfig(1, 2, 1, 1, 70_000, 4));
    }
}
