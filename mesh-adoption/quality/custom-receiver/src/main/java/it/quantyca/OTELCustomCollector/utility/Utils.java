package it.quantyca.OTELCustomCollector.utility;

import io.opentelemetry.proto.common.v1.AnyValue;
import org.slf4j.Logger;

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
}
