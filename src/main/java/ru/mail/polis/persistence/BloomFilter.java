package ru.mail.polis.persistence;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.jetbrains.annotations.NotNull;

public class BloomFilter {
    private BloomFilter() {
    }

    private static final int[] PRIMES = new int[]{29, 31, 37, 43, 47, 113, 211, 61, 89};

    public static void setKeyToFilter(final BitSet bitSet, final ByteBuffer key) {
        bitSet.or(myHashFunction(key));
    }

    /**
     * This function solve the hashCode for each of the primes
     * and after each do << for this BitSet.
     *
     * @param key ByteBuffer.
     * @return BitSet of this key.
     */
    public static BitSet myHashFunction(@NotNull final ByteBuffer key) {
        final BitSet bitSet = new BitSet();
        int bit = 0;

        for (int prime : PRIMES) {
            int hashCode = 1;
            final int p = key.position();
            for (int i = key.limit() - 1; i >= p; i--) {
                hashCode = prime * hashCode + (int) key.get(i);
            }

            final String s = Integer.toBinaryString(hashCode);
            for (int i = 0; i < s.length(); i++, bit++) {
                if (s.charAt(i) == '1') {
                    bitSet.set(bit);
                }
            }
        }
        return bitSet;
    }

    public static boolean canContains(final BitSet bloomFilter, final ByteBuffer key) {
        final BitSet hashKey = myHashFunction(key);
        hashKey.or(bloomFilter);
        return bloomFilter.equals(hashKey);
    }
}
