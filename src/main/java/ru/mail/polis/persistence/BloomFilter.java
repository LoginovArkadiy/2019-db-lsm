package ru.mail.polis.persistence;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.jetbrains.annotations.NotNull;

public class BloomFilter {
    private static final int[] PRIMES = new int[]{29, 31, 37, 43, 47, 113, 211, 61, 89};

    public static void setKeyToFilter(final BitSet bitSet, final ByteBuffer key) {
        bitSet.or(myHashFunction(key));
    }

    public static BitSet myHashFunction(@NotNull final ByteBuffer key) {
        final BitSet bitSet = new BitSet();
        int bit = 0;

        for (int j = 0; j < PRIMES.length; j++) {
            int h = 1;
            final int p = key.position();
            for (int i = key.limit() - 1; i >= p; i--) {
                h = PRIMES[j] * h + (int) key.get(i);
            }

            final String s = Integer.toBinaryString(h);
            for (int i = 0; i < s.length(); i++, bit++) {
                if (s.charAt(i) == '1') {
                    bitSet.set(bit);
                }
            }
        }
        return bitSet;
    }
}
