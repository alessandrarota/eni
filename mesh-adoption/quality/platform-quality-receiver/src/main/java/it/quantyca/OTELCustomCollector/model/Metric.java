package it.quantyca.OTELCustomCollector.model;

import jakarta.annotation.Nullable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity(name = "Metric")
@Table(name = "metric_current")
@IdClass(MetricID.class)
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class Metric {
    private String businessDomainName;
    @Id private String dataProductName;
    @Id private String expectationName;
    @Id private String dataSourceName;
    @Id private String dataAssetName;
    @Id private String columnName;
    private String blindataSuiteName;
    private String gxSuiteName;
    private String dataQualityDimensionName;
    private Double metricValue;
    private String unitOfMeasure;
    private Integer checkedElementsNbr;
    private Integer errorsNbr;
    @Id private String appName;
    @Id private LocalDateTime otlpSendingDatetime;
    private String statusCode;
    private String lockingServiceCode;
    private LocalDateTime insertDatetime;
    private LocalDateTime updateDatetime;

    @Override
    public String toString() {
        return String.format(
                "Metric[DP='%s', Expectation='%s', Column='%s', Suite='%s', App='%s', Value='%f %s', TS='%s']",
                dataProductName, expectationName, columnName, blindataSuiteName, appName, metricValue, unitOfMeasure, otlpSendingDatetime.toString()
        );
    }
}
