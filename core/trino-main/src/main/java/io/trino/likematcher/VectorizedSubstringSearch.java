/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.likematcher;

import com.google.common.base.StandardSystemProperty;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;

import static io.trino.util.MachineInfo.readCpuFlags;
import static java.lang.Long.numberOfTrailingZeros;
import static java.util.Locale.ENGLISH;

/**
 * Substring search using the Vector API. For each block of candidate start positions, the first and the last byte of
 * the pattern are compared with the input at the same time, and only the positions where both match are verified with
 * a full comparison (http://0x80.pl/notesen/2016-11-28-simd-strfind.html).
 */
final class VectorizedSubstringSearch
{
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int LANES = SPECIES.length();
    private static final boolean SUPPORTED = detectSupport();

    private VectorizedSubstringSearch() {}

    /**
     * The vectorized search is used on x86 CPUs with AVX2, where byte vectors are at least 256 bits wide and
     * the mask of a byte comparison is extracted with a single instruction.
     */
    private static boolean detectSupport()
    {
        String architecture = StandardSystemProperty.OS_ARCH.value();
        if (architecture == null) {
            return false;
        }
        architecture = architecture.toLowerCase(ENGLISH);
        boolean x86 = architecture.contains("amd64") || architecture.contains("x86");
        return x86 && SPECIES.vectorBitSize() >= 256 && LANES <= Long.SIZE && readCpuFlags().contains("avx2");
    }

    /**
     * Returns whether the vectorized search should be used for an input of the given length.
     * Shorter inputs do not fill a single vector and are faster with the scalar search.
     */
    public static boolean shouldUse(int patternLength, int inputLength)
    {
        return SUPPORTED && inputLength >= patternLength - 1 + LANES;
    }

    /**
     * Returns the position of the first occurrence of the pattern in the input range, or -1 if
     * the pattern does not occur in the range or is empty.
     */
    public static int find(byte[] input, int offset, int length, byte[] pattern)
    {
        int patternLength = pattern.length;
        if (patternLength > length || patternLength == 0) {
            return -1;
        }

        byte first = pattern[0];
        byte last = pattern[patternLength - 1];
        // the last position where the pattern can start
        int lastStart = offset + length - patternLength;

        int position = offset;
        if (lastStart - position + 1 >= LANES) {
            ByteVector firstVector = ByteVector.broadcast(SPECIES, first);
            ByteVector lastVector = ByteVector.broadcast(SPECIES, last);
            int vectorLimit = lastStart - LANES + 1;
            for (; position <= vectorLimit; position += LANES) {
                ByteVector firstBytes = ByteVector.fromArray(SPECIES, input, position);
                ByteVector lastBytes = ByteVector.fromArray(SPECIES, input, position + patternLength - 1);
                long candidates = firstBytes.eq(firstVector).and(lastBytes.eq(lastVector)).toLong();
                while (candidates != 0) {
                    int start = position + numberOfTrailingZeros(candidates);
                    if (Arrays.equals(input, start, start + patternLength, pattern, 0, patternLength)) {
                        return start;
                    }
                    candidates &= candidates - 1;
                }
            }
        }

        for (; position <= lastStart; position++) {
            if (input[position] == first &&
                    input[position + patternLength - 1] == last &&
                    Arrays.equals(input, position, position + patternLength, pattern, 0, patternLength)) {
                return position;
            }
        }
        return -1;
    }
}
