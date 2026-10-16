package demo.platform.common;

/** Bounds list endpoints so a single request cannot scan an unbounded result set. */
public final class PageLimits {
    public static final int MAX = 200;

    private PageLimits() {
    }

    public static int require(int limit) {
        if (limit < 1 || limit > MAX)
            throw new IllegalArgumentException("limit must be between 1 and " + MAX);
        return limit;
    }
}
