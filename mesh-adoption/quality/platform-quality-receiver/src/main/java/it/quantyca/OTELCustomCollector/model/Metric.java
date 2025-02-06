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
    private String dataProductName;
    @Id private String checkName;
    private Double metricValue;
    private String unitOfMeasure;
    private Integer checkedElementsNbr;
    private Integer errorsNbr;
    private String metricSourceName;
    private String statusCode;
    private String lockingServiceCode;
    @Id private String otlpSendingDatetimeCode;
    private LocalDateTime otlpSendingDatetime;
    private LocalDateTime insertDatetime;
    private LocalDateTime updateDatetime;

    @Override
    public String toString() {
        return String.format(
                "Metric[DP='%s', Check='%s', Value='%f %s', TS='%s']",
                dataProductName, checkName, metricValue, unitOfMeasure, otlpSendingDatetimeCode
        );
    }
}
