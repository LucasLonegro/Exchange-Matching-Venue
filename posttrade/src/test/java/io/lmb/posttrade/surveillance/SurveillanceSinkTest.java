package io.lmb.posttrade.surveillance;

import io.lmb.common.Events;
import io.lmb.posttrade.PostTradeMetrics;
import io.lmb.posttrade.SinkRunner;
import io.lmb.posttrade.TestVenue;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.TradeDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SurveillanceSinkTest
{
    private static final Pattern KEY = Pattern.compile("^\\{\"shard\":(\\d+),\"eventSeq\":(\\d+),\"type\":\"(\\w+)\".*}$");

    private final TestVenue venue = new TestVenue(2).standardSetup();
    private final PostTradeMetrics metrics = new PostTradeMetrics();
    private final Path file = Path.of("target", "test-data", "surv-" + System.nanoTime(), "surveillance.jsonl");
    private SinkRunner runner;

    @AfterEach
    void tearDown()
    {
        if (runner != null)
        {
            runner.close();
        }
    }

    private void start()
    {
        runner = new SinkRunner(new SurveillanceSink(new JsonLinesOutput(file), 20, metrics), venue, 64, 50).start();
    }

    private void stop()
    {
        runner.close();
        runner = null;
    }

    private void awaitApplied()
    {
        TestVenue.await(() -> runner.lastEventSeq(0) == venue.venue.eventCount(0) &&
            runner.lastEventSeq(1) == venue.venue.eventCount(1), 10_000, "surveillance to apply every event");
    }

    private void trade()
    {
        venue.cross(0, 10_000, 5);
        venue.cross(1, 10_100, 7);
        final long id = venue.order(0, 100, Side.BUY, 9_000, 3);
        venue.cancel(0, 100, id);
    }

    /** "shard:eventSeq:type" of every ExecutionReport and Trade the venue emitted. */
    private Set<String> expected()
    {
        final Set<String> out = new HashSet<>();
        for (int s = 0; s < 2; s++)
        {
            for (long seq = 1; seq <= venue.venue.eventCount(s); seq++)
            {
                final int t = Events.templateId(new UnsafeBuffer(venue.venue.event(s, seq)), 0);
                if (t == ExecutionReportDecoder.TEMPLATE_ID || t == TradeDecoder.TEMPLATE_ID)
                {
                    out.add(s + ":" + seq + ":" + (t == TradeDecoder.TEMPLATE_ID ? "Trade" : "ExecutionReport"));
                }
            }
        }
        return out;
    }

    /** Asserts the file holds exactly the expected records, each once, every line a complete object. */
    private List<String> assertFile() throws IOException
    {
        final List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        final List<String> keys = new ArrayList<>();
        for (final String line : lines)
        {
            final Matcher m = KEY.matcher(line);
            assertTrue(m.matches(), "malformed line: " + line);
            keys.add(m.group(1) + ":" + m.group(2) + ":" + m.group(3));
        }
        assertEquals(keys.size(), new HashSet<>(keys).size(), "a record was written twice");
        assertEquals(expected(), new HashSet<>(keys));
        return lines;
    }

    @Test
    void writesOneJsonLinePerReportAndTrade() throws IOException
    {
        trade();
        start();
        awaitApplied();
        stop();
        final List<String> lines = assertFile();
        final String tradeLine = lines.stream().filter(l -> l.contains("\"type\":\"Trade\"") && l.startsWith("{\"shard\":0,"))
            .findFirst().orElseThrow();
        assertTrue(tradeLine.contains("\"price\":10000") && tradeLine.contains("\"quantity\":5") &&
            tradeLine.contains("\"buyFirmId\":100") && tradeLine.contains("\"sellFirmId\":200"), tradeLine);
        assertTrue(lines.stream().anyMatch(l -> l.contains("\"execType\":\"CANCELED\"") && l.contains("\"firmId\":100")));
        assertTrue(lines.stream().anyMatch(l -> l.contains("\"execType\":\"NEW\"")), "every exec type is recorded");
        assertEquals(lines.size(), metrics.surveillanceRecords.get());
    }

    @Test
    void restartRecoversFromTheFileItselfAfterLostAndTornWrites() throws IOException
    {
        trade();
        start();
        awaitApplied();
        stop();
        final List<String> lines = assertFile();

        // simulate a crash: the last three records never reached the disk and the next one was half written
        final List<String> kept = lines.subList(0, lines.size() - 3);
        final String torn = lines.get(lines.size() - 3).substring(0, 25);
        Files.writeString(file, String.join("\n", kept) + "\n" + torn, StandardCharsets.US_ASCII,
            StandardOpenOption.TRUNCATE_EXISTING);

        venue.rewind = 5; // at-least-once redelivery from the stream must not duplicate either
        trade();
        start();
        awaitApplied();
        trade(); // live after the restart
        awaitApplied();
        stop();
        assertFile();
    }
}
