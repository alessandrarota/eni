package it.quantyca.OTELCustomCollector.model;

import jakarta.persistence.Column;
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
    @Column(name = "data_product_name")
    private String dataProductName;

    @Id
    @Column(name = "app_name")
    private String appName;

    @Id
    @Column(name = "expectation_name")
    private String expectationName;

    @Id
    @Column(name = "metric_name")
    private String metricName;

    @Column(name = "metric_description")
    private String metricDescription;

    @Column(name = "value")
    private Double value;

    @Column(name = "unit_of_measure")
    private String unitOfMeasure;

    @Column(name = "element_count")
    private Integer elementCount;

    @Column(name = "unexpected_count")
    private Integer unexpectedCount;

    @Id
    @Column(name = "timestamp")
    private String timestamp;

    @Override
    public String toString() {
        return String.format(
                "Metric[DP='%s', App='%s', Metric='%s', Value='%f %s', Timestamp='%s']",
                dataProductName, appName, metricName, value, unitOfMeasure, timestamp
        );
    }
}
