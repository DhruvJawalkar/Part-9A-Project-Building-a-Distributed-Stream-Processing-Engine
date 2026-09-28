package dev.dhruv.streaming.lms;

import java.io.Serializable;

/**
 * The key shared by a result click and the borrow it may have produced.
 *
 * <p>Member alone is too broad: one member may click several catalog items inside the same
 * 30-minute interval. Item alone is also too broad: two members borrowing the same popular
 * title are unrelated conversions. The pair is the smallest key under which every possible
 * match is local and every impossible cross-member or cross-item match is excluded.
 *
 * @param memberId      member whose action is being joined
 * @param catalogItemId item clicked or borrowed
 */
public record ConversionKey(String memberId, String catalogItemId) implements Serializable {

    private static final long serialVersionUID = 1L;
}
