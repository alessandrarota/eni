package it.quantyca.OTELCustomCollector.service;

import io.grpc.stub.StreamObserver;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import it.quantyca.OTELCustomCollector.model.*;
import it.quantyca.OTELCustomCollector.repository.MetricRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.primitives.Ints;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static it.quantyca.OTELCustomCollector.utility.Utils.*;

@Service
public class OTELMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase {
    @Autowired
    private MetricRepository metricRepository;

    private Logger logger = LoggerFactory.getLogger(OTELMetricsService.class);

    @Value("${otlp.metric.status_value:NEW}")
    private String STATUS_VALUE;

    @Value("${otlp.metric.signal_datetime_code_format:yyyyMMddHHmmss}")
    private String SIGNAL_DATETIME_CODE_FORMAT;

    @Value("${otlp.metric.appName.key:service.name}")
    private String APP_NAME_KEY;
    @Value("${otlp.metric.appName.placeholder:N/D}")
    private String APP_NAME_PLACEHOLDER;

    @Value("${otlp.metric.signalType.key:signal_type}")
    private String SIGNAL_TYPE_KEY;
    @Value("${otlp.metric.signalType.filterValue:DATA_QUALITY}")
    private String SIGNAL_TYPE_FILTER_VALUE;

    @Value("${otlp.metric.checkName.key:check_name}")
    private String CHECK_NAME_KEY;
    @Value("${otlp.metric.dataProductName.key:data_product_name}")
    private String DATA_PRODUCT_NAME_KEY;
    @Value("${otlp.metric.elementCount.key:element_count}")
    private String ELEMENT_COUNT_KEY;
    @Value("${otlp.metric.elementCount.placeholder:0}")
    private String ELEMENT_COUNT_PLACEHOLDER;
    @Value("${otlp.metric.unexpectedCount.key:unexpected_count}")
    private String UNEXPECTED_COUNT_KEY;
    @Value("${otlp.metric.unexpectedCount.placeholder:0}")
    private String UNEXPECTED_COUNT_PLACEHOLDER;



    @Override
    @Transactional
    public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        logger.info("Processing new METRIC");
        logger.debug(request.toString());

        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            resourceMetrics.getScopeMetricsList().forEach(scopeMetric -> {
                scopeMetric.getMetricsList().forEach(currentMetric -> {
                    List<NumberDataPoint> dataPoints;
                    if (currentMetric.getDataCase() == io.opentelemetry.proto.metrics.v1.Metric.DataCase.GAUGE) {
                        dataPoints = currentMetric.getGauge().getDataPointsList();
                    } else if (currentMetric.getDataCase() == io.opentelemetry.proto.metrics.v1.Metric.DataCase.SUM) {
                        dataPoints = currentMetric.getSum().getDataPointsList();
                    } else {
                        logger.warn("Wasting metric... Current DataCase (" + currentMetric.getDataCase() + ") not managed.");
                        dataPoints = new ArrayList<NumberDataPoint>();
                    }

                    dataPoints.forEach(dataPoint -> {
                        LocalDateTime dataPointDatetime = LocalDateTime.ofInstant(
                                Instant.ofEpochSecond(
                                        dataPoint.getTimeUnixNano() / 1_000_000_000,
                                        (int) (dataPoint.getTimeUnixNano() % 1_000_000_000)
                                ),
                                ZoneId.systemDefault()
                        );

                        String signalTypeValue = "";

                        String attr_checkName = null;
                        String attr_dataProductName = null;
                        String attr_elementCount = ELEMENT_COUNT_PLACEHOLDER;
                        String attr_unexpectedCount = UNEXPECTED_COUNT_PLACEHOLDER;

                        String appName = getValueDataFromOptionalAnyValue(
                                resourceMetrics.getResource().getAttributesList().stream()
                                        .filter(kv -> kv.getKey().equals(APP_NAME_KEY))
                                        .map(KeyValue::getValue)
                                        .findFirst(),
                                APP_NAME_PLACEHOLDER,
                                logger
                        );

                        for (KeyValue attribute : dataPoint.getAttributesList()) {
                            if (attribute.getKey().equalsIgnoreCase(SIGNAL_TYPE_KEY)) {
                                signalTypeValue = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(CHECK_NAME_KEY)) {
                                attr_checkName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(DATA_PRODUCT_NAME_KEY)) {
                                attr_dataProductName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(ELEMENT_COUNT_KEY)) {
                                attr_elementCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(UNEXPECTED_COUNT_KEY)) {
                                attr_unexpectedCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            }
                        }

                        if (! signalTypeValue.equalsIgnoreCase(SIGNAL_TYPE_FILTER_VALUE)) {
                            logger.warn("Wasting metric... " +
                                    "Signal type expected: [" + SIGNAL_TYPE_FILTER_VALUE + "] " +
                                    "Signal type received: [" + signalTypeValue + "]");
                        } else {
                            logger.warn("Storing new metric data point.");
                            metricRepository.save(new Metric(
                                    cleanStringCamelCase(attr_dataProductName),
                                    attr_checkName,
                                    dataPoint.getAsDouble() == 0.0 ? dataPoint.getAsInt() : dataPoint.getAsDouble(),
                                    currentMetric.getUnit(),
                                    Optional.ofNullable(attr_elementCount).map(Ints::tryParse).orElse(null),
                                    Optional.ofNullable(attr_unexpectedCount).map(Ints::tryParse).orElse(null),
                                    appName,
                                    STATUS_VALUE,
                                    null,
                                    dataPointDatetime.format(DateTimeFormatter.ofPattern(SIGNAL_DATETIME_CODE_FORMAT)),
                                    dataPointDatetime,
                                    LocalDateTime.now(),
                                    LocalDateTime.now()
                            ));
                        }
                    });
                });
            });
        }
        responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}