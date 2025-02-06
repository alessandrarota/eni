package it.quantyca.OTELCustomCollector.model;

import java.io.Serializable;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class MetricID implements Serializable {
    private String checkName;
    private String otlpSendingDatetimeCode;

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof MetricID that)) return false;

        return this.checkName.equals(that.getCheckName()) &&
                this.otlpSendingDatetimeCode.equals(that.getOtlpSendingDatetimeCode());
    }

    @Override
    public final int hashCode() {
        return Objects.hash(
                this.checkName,
                this.otlpSendingDatetimeCode
        );
    }
}
