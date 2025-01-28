package it.quantyca.OTELCustomCollector.utility;

import io.opentelemetry.proto.common.v1.AnyValue;
import org.slf4j.Logger;

import java.util.Optional;

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

    public static String getValueDataFromOptionalAnyValue(Optional<AnyValue> value, Logger logger) {
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
            return "";
        }
    }
}
