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
    private String expectationName;
    private String dataSourceName;
    private String dataAssetName;
    private String columnName;
    private String appName;
    private LocalDateTime otlpSendingDatetime;

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof MetricID that)) return false;

        return this.dataProductName.equals(that.getDataProductName()) &&
                this.expectationName.equals(that.getExpectationName()) &&
                this.dataSourceName.equals(that.getDataSourceName()) &&
                this.dataAssetName.equals(that.getDataAssetName()) &&
                this.columnName.equals(that.getColumnName()) &&
                this.appName.equals(that.getAppName()) &&
                this.otlpSendingDatetime.equals(that.getOtlpSendingDatetime());
    }

    @Override
    public final int hashCode() {
        return Objects.hash(
                this.dataProductName,
                this.expectationName,
                this.dataSourceName,
                this.dataAssetName,
                this.columnName,
                this.appName,
                this.otlpSendingDatetime
        );
    }
}
