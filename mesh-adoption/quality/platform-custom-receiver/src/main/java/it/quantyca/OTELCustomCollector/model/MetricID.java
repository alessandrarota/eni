package it.quantyca.OTELCustomCollector.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class MetricID implements Serializable {
    private String dataProductName;
    private String appName;
    private String expectationName;
    private String metricName;
    private String timestamp;

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof MetricID that)) return false;

        return this.dataProductName.equals(that.getDataProductName()) &&
                this.appName.equals(that.getAppName()) &&
                this.expectationName.equals(that.getExpectationName()) &&
                this.metricName.equals(that.getMetricName()) &&
                this.timestamp.equals(that.getTimestamp());
    }

    @Override
    public final int hashCode() {
        return Objects.hash(
                this.dataProductName,
                this.appName,
                this.expectationName,
                this.metricName,
                this.timestamp
        );
    }
}
