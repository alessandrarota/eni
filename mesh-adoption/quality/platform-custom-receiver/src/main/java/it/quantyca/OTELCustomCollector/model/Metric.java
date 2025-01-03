package it.quantyca.OTELCustomCollector.model;

import java.time.LocalDateTime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity(name = "Metric")
@Table(name = "metric_current")
@IdClass(MetricID.class)
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class Metric {
    @Id
    private String dataProductName;
    @Id
    private String appName;
    @Id
    private String expectationName;
    @Id
    private String metricName;
    private String metricDescription;
    private Double value;
    private String unitOfMeasure;
    private Integer elementCount;
    private Integer unexpectedCount;
    @Id
    private String timestamp;

    @Override
    public String toString() {
        return String.format(
                "Metric[DP='%s', App='%s', Metric='%s', Value='%f %s', Timestamp='%s']",
                dataProductName, appName, metricName, value, unitOfMeasure, timestamp
        );
    }
}
