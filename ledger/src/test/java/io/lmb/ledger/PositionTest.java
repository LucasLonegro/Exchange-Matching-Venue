package io.lmb.ledger;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PositionTest
{
    private static Position pos()
    {
        return new Position(100, 1, 0);
    }

    @Test
    void addingToALongAveragesTheCost()
    {
        final Position p = pos();
        p.fill(true, 10_000, 10);
        p.fill(true, 10_600, 5);
        assertEquals(15, p.quantity);
        assertEquals(153_000, p.costBasis);
        assertEquals(0, p.realisedPnl);
        assertEquals(-153_000, p.cash);
        assertEquals("10200.0000", StatementWriter.averageCost(p.costBasis, p.quantity));
    }

    @Test
    void reducingALongRealisesAgainstTheAverageCost()
    {
        final Position p = pos();
        p.fill(true, 10_000, 10);
        p.fill(false, 10_100, 4);
        assertEquals(6, p.quantity);
        assertEquals(60_000, p.costBasis);
        assertEquals(400, p.realisedPnl);
        assertEquals(400, p.dayRealisedPnl);
        assertEquals(-59_600, p.cash);
        assertEquals(600, p.unrealisedPnl(10_100));
    }

    @Test
    void coveringAShortRealisesTheMirrorImage()
    {
        final Position p = pos();
        p.fill(false, 10_000, 10);
        p.fill(true, 10_100, 4);
        assertEquals(-6, p.quantity);
        assertEquals(-60_000, p.costBasis);
        assertEquals(-400, p.realisedPnl);
        assertEquals(59_600, p.cash);
        assertEquals(-600, p.unrealisedPnl(10_100));
        assertEquals("10000.0000", StatementWriter.averageCost(p.costBasis, p.quantity));
    }

    @Test
    void crossingZeroClosesEverythingThenOpensTheRestAtTheFillPrice()
    {
        final Position p = pos();
        p.fill(true, 10_000, 10);
        p.fill(false, 9_900, 15);
        assertEquals(-5, p.quantity);
        assertEquals(-49_500, p.costBasis);
        assertEquals(-1_000, p.realisedPnl);
        assertEquals(148_500 - 100_000, p.cash);
    }

    @Test
    void truncationRemainderLeavesWithTheLastShare()
    {
        final Position p = pos();
        p.fill(true, 100, 1);
        p.fill(true, 100, 1);
        p.fill(true, 101, 1);
        p.fill(false, 100, 1); // removes 301 * 1 / 3 = 100 (truncated): realised 0
        assertEquals(0, p.realisedPnl);
        assertEquals(201, p.costBasis);
        p.fill(false, 100, 2); // the last two shares carry the remaining 201
        assertEquals(0, p.quantity);
        assertEquals(0, p.costBasis);
        assertEquals(-1, p.realisedPnl);
        assertEquals(p.realisedPnl, p.cash);
    }

    @Test
    void cashIsRealisedMinusCostBasisAfterAnyFills()
    {
        final SplittableRandom r = new SplittableRandom(7);
        for (int run = 0; run < 200; run++)
        {
            final Position p = pos();
            for (int i = 0; i < 200; i++)
            {
                p.fill(r.nextBoolean(), 9_000 + r.nextInt(2_000), 1 + r.nextInt(500));
                assertEquals(p.cash, p.realisedPnl - p.costBasis, "run " + run + " fill " + i);
                if (p.quantity == 0)
                {
                    assertEquals(0, p.costBasis);
                }
                else
                {
                    assertEquals(Long.signum(p.quantity), Long.signum(p.costBasis));
                }
            }
            final long mark = 10_000;
            assertEquals(p.cash + p.marketValue(mark), p.realisedPnl + p.unrealisedPnl(mark));
        }
    }

    @Test
    void closeDayReportsTheDayAndCarriesThePosition()
    {
        final Position p = pos();
        p.fill(true, 10_000, 10);
        final Position.DailyPosition d1 = p.closeDay(20261127, 10_200, "LAST_TRADE");
        assertEquals(0, d1.openQty());
        assertEquals(10, d1.closeQty());
        assertEquals(102_000, d1.marketValue());
        assertEquals(2_000, d1.unrealisedPnl());
        assertEquals(2_000, d1.pnlDay());
        assertEquals(-100_000, d1.cashFlowDay());
        assertEquals(0, p.dayBoughtQty);
        assertEquals(10, p.dayOpenQty);

        p.fill(false, 10_300, 10);
        final Position.DailyPosition d2 = p.closeDay(20261130, 10_300, "LAST_TRADE");
        assertEquals(10, d2.openQty());
        assertEquals(0, d2.closeQty());
        assertEquals(3_000, d2.realisedPnlDay());
        assertEquals(1_000, d2.pnlDay(), "103000 sold - 102000 carried in");
        assertEquals(d1.pnlDay() + d2.pnlDay(), d2.realisedPnlTotal());
    }

    @Test
    void aPositionWithoutAMarkIsValuedAtCost()
    {
        final Position p = pos();
        p.fill(true, 10_000, 3);
        assertEquals(30_000, p.marketValue(Ledger.NULL_PRICE));
        assertEquals(0, p.unrealisedPnl(Ledger.NULL_PRICE));
    }

    @Test
    void arithmeticIsExact()
    {
        assertEquals(3, Position.mulDiv(Long.MAX_VALUE, 3, Long.MAX_VALUE));
        assertEquals(-3, Position.mulDiv(Long.MIN_VALUE + 1, 3, Long.MAX_VALUE));
        assertEquals(7, Position.mulDiv(10, 7, 10));
        final Position p = pos();
        assertThrows(ArithmeticException.class, () -> p.fill(true, Long.MAX_VALUE / 2, 3));
        assertThrows(IllegalArgumentException.class, () -> p.fill(true, 100, 0));
    }
}
