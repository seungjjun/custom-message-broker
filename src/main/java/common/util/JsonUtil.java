package common.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static byte[] toBytes(Object object) {
        try {
            return MAPPER.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert object to bytes", e);
        }
    }

    public static <T> T fromBytes(byte[] b, Class<T> c) {
        try {
            return MAPPER.readValue(b, c);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert bytes to object", e);
        }
    }

    private JsonUtil() {
        // Prevent instantiation
    }
}
