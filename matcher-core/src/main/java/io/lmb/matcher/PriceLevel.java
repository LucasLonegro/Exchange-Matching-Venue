package io.lmb.matcher;

/** One price level of one book side: total quantity and an intrusive FIFO of orders. */
final class PriceLevel
{
    long price;
    long totalQty;
    final OrderList orders = new OrderList(Order.L_LEVEL);
    BookSide side;
    /** In the side's sorted array of non-empty levels. */
    boolean attached;
    /** In the dirty list of the current step; baseQty/baseCount hold the values at first touch. */
    boolean dirty;
    long baseQty;
    int baseCount;
    PriceLevel nextFree;

    int count()
    {
        return orders.size;
    }
}
