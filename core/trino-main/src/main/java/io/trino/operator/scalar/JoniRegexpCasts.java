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
package io.trino.operator.scalar;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.jcodings.specific.ASCIIEncoding;
import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Syntax;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.type.JoniRegexp;
import io.trino.type.JoniRegexpType;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Chars.padSpaces;

public final class JoniRegexpCasts
{
    // character class shorthands, anchors, control characters and quoting
    private static final String SAFE_ESCAPES = "dDwWsSbBAzZGtnrfaeQE";

    private JoniRegexpCasts() {}

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(OperatorType.CAST)
    @SqlType(JoniRegexpType.NAME)
    public static JoniRegexp castVarcharToJoniRegexp(@SqlType("varchar(x)") Slice pattern)
    {
        return joniRegexp(pattern);
    }

    // fallible
    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(JoniRegexpType.NAME)
    public static JoniRegexp castCharToJoniRegexp(@LiteralParameter("x") int charLength, @SqlType("char(x)") Slice pattern)
    {
        return joniRegexp(padSpaces(pattern, charLength));
    }

    public static JoniRegexp joniRegexp(Slice pattern)
    {
        Regex regex;
        try {
            // When normal UTF8 encoding instead of non-strict UTF8) is used, joni can infinite loop when invalid UTF8 slice is supplied to it.
            regex = new Regex(pattern.getBytes(), 0, pattern.length(), Option.DEFAULT, NonStrictUTF8Encoding.INSTANCE, Syntax.Java);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        Regex asciiRegex = null;
        if (isAsciiCompatible(pattern)) {
            try {
                asciiRegex = new Regex(pattern.getBytes(), 0, pattern.length(), Option.DEFAULT, ASCIIEncoding.INSTANCE, Syntax.Java);
            }
            catch (RuntimeException e) {
                // the regex compiled for UTF-8 is used for all input
            }
        }
        return new JoniRegexp(pattern, regex, asciiRegex);
    }

    /**
     * Whether the pattern matches ASCII input in a single byte encoding exactly like in UTF-8. This
     * holds for patterns of ASCII characters, unless their meaning depends on Unicode: case insensitive
     * matching and other inline options, POSIX bracket classes and Unicode properties, which classify
     * some ASCII characters differently, and escapes of arbitrary code points. Escapes that are not
     * known to be safe are rejected.
     */
    @VisibleForTesting
    static boolean isAsciiCompatible(Slice pattern)
    {
        for (int i = 0; i < pattern.length(); i++) {
            byte value = pattern.getByte(i);
            if (value < 0) {
                return false;
            }
            if (value == '\\') {
                i++;
                if (i == pattern.length()) {
                    return false;
                }
                byte escaped = pattern.getByte(i);
                if (isAsciiLetterOrDigit(escaped) && SAFE_ESCAPES.indexOf(escaped) < 0) {
                    return false;
                }
            }
            else if (value == '[' && i + 1 < pattern.length() && pattern.getByte(i + 1) == ':') {
                return false;
            }
            else if (value == '(' && i + 2 < pattern.length() && pattern.getByte(i + 1) == '?' && !isGroupConstruct(pattern.getByte(i + 2))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isGroupConstruct(byte value)
    {
        // non-capturing group, lookahead, atomic group and lookbehind; named groups start with a letter after '<'
        return value == ':' || value == '=' || value == '!' || value == '>' || value == '<';
    }

    private static boolean isAsciiLetterOrDigit(byte value)
    {
        return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9');
    }
}
