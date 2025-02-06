package it.quantyca.OTELCustomCollector.utility;

import java.text.Normalizer;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import io.opentelemetry.proto.common.v1.AnyValue;

public class Utils {
    public static String getValueDataFromAnyValue(AnyValue value, Logger logger) {
        if (value.hasStringValue()) {
            return value.getStringValue();
        } else if (value.hasBoolValue()) {
            return String.valueOf(value.getBoolValue());
        } else if (value.hasIntValue()) {
            return String.valueOf(value.getIntValue());
        } else if (value.hasDoubleValue()) {
            return String.valueOf(value.getDoubleValue());
        } else {
            logger.warn("Unhandled type in Metric Attribute Value.");
            return value.toString();
        }
    }

    public static String getValueDataFromOptionalAnyValue(Optional<AnyValue> value, String placeholder, Logger logger) {
        if (value.isPresent()) {
            if (value.get().hasStringValue()) {
                return value.get().getStringValue();
            } else if (value.get().hasBoolValue()) {
                return String.valueOf(value.get().getBoolValue());
            } else if (value.get().hasIntValue()) {
                return String.valueOf(value.get().getIntValue());
            } else if (value.get().hasDoubleValue()) {
                return String.valueOf(value.get().getDoubleValue());
            } else {
                logger.warn("Unhandled type in Metric Attribute Value.");
                return value.toString();
            }
        } else {
            logger.warn("Metric Attribute Value is not present.");
            return placeholder;
        }
    }

    public static String cleanStringCamelCase(String originalValue) {
        if (originalValue == null) {
            return null;
        } else {
            // Normalize to remove accents
            String normalized = Normalizer.normalize(originalValue, Normalizer.Form.NFD).replaceAll("\\p{M}", "");
            // Remove non-alphabetic characters (except spaces for later processing)
            String cleaned = normalized.replaceAll("[^a-zA-Z\s]", "").strip();
            // Check if the string contains blank
            if (cleaned.contains(" ")) {
                // Convert to camelCase
                StringBuilder camelCaseString = new StringBuilder();
                boolean capitalizeNext = false;
                for (char c : cleaned.toLowerCase().toCharArray()) {
                    if (c == ' ') {
                        capitalizeNext = true;
                    } else {
                        if (capitalizeNext) {
                            camelCaseString.append(Character.toUpperCase(c));
                            capitalizeNext = false;
                        } else {
                            camelCaseString.append(c);
                        }
                    }
                }
                return camelCaseString.toString();
            } else {
                return cleaned;
            }
        }
    }

    public static String reconductBusinessDomain(String originalValue, Map<String, String> businessDomains) {
        if (originalValue == null) {
            return null;
        } else {
            return businessDomains.get(cleanStringCamelCase(originalValue).toUpperCase());
        }
    }
}
