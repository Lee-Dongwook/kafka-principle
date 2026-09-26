package io.github.kafkaprinciple.common.utils.internals;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import javax.management.ObjectName;

public class Sanitizer {
    private static final Pattern MBEAN_PATTERN = Pattern.compile("[\\w-%\\. \t]*");

    public static String sanitize(String name) {
        String encoded = URLEncoder.encode(name, StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < encoded.length(); i++) {
            char c = encoded.charAt(i);

            if (c == '*') {
                builder.append("%2A");
            } else if (c == '+') {
                builder.append("%20");
            } else {
                builder.append(c);
            }
        }

        return builder.toString();
    }

    public static String desanitize(String name) {
        return URLDecoder.decode(name, StandardCharsets.UTF_8);
    }

    public static String jmxSanitize(String name) {
        return MBEAN_PATTERN.matcher(name).matches() ? name : ObjectName.quote(name);
    }

}
