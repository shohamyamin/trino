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

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVectorizedSubstringSearch
{
    private static final String[] ALPHABET = {"a", "b", "g", "o", "é", "€", "😀"};

    @Test
    public void testRandomInputs()
    {
        Random random = new Random(42);
        for (int iteration = 0; iteration < 20_000; iteration++) {
            byte[] pattern = randomString(random, 1 + random.nextInt(random.nextBoolean() ? 4 : 80)).getBytes(UTF_8);
            byte[] value = randomString(random, random.nextInt(200)).getBytes(UTF_8);
            if (random.nextBoolean() && value.length >= pattern.length) {
                // plant the pattern at a random position, including the start and the end of the value
                int position = switch (random.nextInt(3)) {
                    case 0 -> 0;
                    case 1 -> value.length - pattern.length;
                    default -> random.nextInt(value.length - pattern.length + 1);
                };
                System.arraycopy(pattern, 0, value, position, pattern.length);
            }
            assertSameAsScalar(value, pattern, random.nextInt(5), random.nextInt(5));
        }
    }

    @Test
    public void testLaneBoundaries()
    {
        byte[] pattern = "google".getBytes(UTF_8);
        for (int length = 0; length <= 200; length++) {
            for (int position = 0; position + pattern.length <= length; position++) {
                byte[] value = "x".repeat(length).getBytes(UTF_8);
                System.arraycopy(pattern, 0, value, position, pattern.length);
                assertSameAsScalar(value, pattern, 0, 0);
                assertSameAsScalar(value, pattern, 3, 7);
            }
        }
    }

    @Test
    public void testEmptyPattern()
    {
        assertThat(VectorizedSubstringSearch.find(new byte[100], 0, 100, new byte[0])).isEqualTo(-1);
        assertThat(VectorizedSubstringSearch.find(new byte[0], 0, 0, new byte[0])).isEqualTo(-1);
    }

    @Test
    public void testLikeMatcher()
    {
        Random random = new Random(7);
        for (int iteration = 0; iteration < 5_000; iteration++) {
            String literal = randomString(random, 1 + random.nextInt(8));
            String value = randomString(random, random.nextInt(150));
            if (random.nextBoolean()) {
                int position = random.nextInt(value.length() + 1);
                value = value.substring(0, position) + literal + value.substring(position);
            }
            String other = randomString(random, 1 + random.nextInt(3));
            byte[] bytes = value.getBytes(UTF_8);

            assertThat(match("%" + literal + "%", bytes)).isEqualTo(value.contains(literal));
            int literalIndex = value.indexOf(literal);
            boolean expected = literalIndex >= 0 && value.indexOf(other, literalIndex + literal.length()) >= 0;
            assertThat(match("%" + literal + "%" + other + "%", bytes)).isEqualTo(expected);
        }
        assertThat(match("%%", new byte[0])).isTrue();
        assertThat(match("", new byte[0])).isTrue();
        assertThat(match("", "a".getBytes(UTF_8))).isFalse();
    }

    private static boolean match(String pattern, byte[] value)
    {
        return LikeMatcher.compile(pattern, Optional.empty()).match(value);
    }

    private static void assertSameAsScalar(byte[] value, byte[] pattern, int leftPadding, int rightPadding)
    {
        byte[] input = new byte[leftPadding + value.length + rightPadding];
        System.arraycopy(value, 0, input, leftPadding, value.length);
        assertThat(VectorizedSubstringSearch.find(input, leftPadding, value.length, pattern))
                .isEqualTo(scalarFind(input, leftPadding, value.length, pattern));
    }

    private static int scalarFind(byte[] input, int offset, int length, byte[] pattern)
    {
        if (pattern.length == 0) {
            return -1;
        }
        for (int start = offset; start + pattern.length <= offset + length; start++) {
            int i = 0;
            while (i < pattern.length && input[start + i] == pattern[i]) {
                i++;
            }
            if (i == pattern.length) {
                return start;
            }
        }
        return -1;
    }

    private static String randomString(Random random, int length)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        }
        return builder.toString();
    }
}
