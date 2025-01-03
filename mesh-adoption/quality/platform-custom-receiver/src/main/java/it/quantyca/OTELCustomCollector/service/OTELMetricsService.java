package it.quantyca.OTELCustomCollector.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.primitives.Ints;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import it.quantyca.OTELCustomCollector.model.Metric;
import it.quantyca.OTELCustomCollector.repository.MetricRepository;
import static it.quantyca.OTELCustomCollector.utility.Utils.getValueDataFromAnyValue;
import static it.quantyca.OTELCustomCollector.utility.Utils.getValueDataFromOptionalAnyValue;
import jakarta.transaction.Transactional;

@Service
public class OTELMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase {
    @Autowired
    private MetricRepository metricRepository;

    private Logger logger = LoggerFactory.getLogger(OTELMetricsService.class);

    @Value("${otlp.signalType:DATA_QUALITY}")
    private String SIGNAL_TYPE_FILTER;

    @Value("${otlp.metric.dataProductName.placeholder:N/A}")
    private String DATA_PRODUCT_NAME_PLACEHOLDER;
    @Value("${otlp.metric.dataProductName.key:data_product_name}")
    private String DATA_PRODUCT_NAME_KEY;
    @Value("${otlp.metric.expectationName.placeholder:N/A}")
    private String EXPECTATION_NAME_PLACEHOLDER;
    @Value("${otlp.metric.expectationName.key:expectation_name}")
    private String EXPECTATION_NAME_KEY;
    @Value("${otlp.metric.elementCount.placeholder:0}")
    private String ELEMENT_COUNT_PLACEHOLDER;
    @Value("${otlp.metric.elementCount.key:element_count}")
    private String ELEMENT_COUNT_KEY;
    @Value("${otlp.metric.unexpectedCount.placeholder:0}")
    private String UNEXPECTED_COUNT_PLACEHOLDER;
    @Value("${otlp.metric.unexpectedCount.key:unexpected_count}")
    private String UNEXPECTED_COUNT_KEY;
    @Value("${otlp.metric.appName.placeholder:N/A}")
    private String APP_NAME_PLACEHOLDER;
    @Value("${otlp.metric.appName.key:app-name}")
    private String APP_NAME_KEY;


    @Override
    @Transactional
    public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        logger.info("Processing new METRIC");
        logger.debug(request.toString());

        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            resourceMetrics.getScopeMetricsList().forEach(scopeMetric -> {
                scopeMetric.getMetricsList().forEach(currentMetric -> {
                    if (currentMetric.getDataCase() == io.opentelemetry.proto.metrics.v1.Metric.DataCase.GAUGE) {
                        currentMetric.getGauge().getDataPointsList().forEach(dataPoint -> {

                            String dataProductName = DATA_PRODUCT_NAME_PLACEHOLDER;
                            String expectationName = EXPECTATION_NAME_PLACEHOLDER;
                            String elementCount = ELEMENT_COUNT_PLACEHOLDER;
                            String unexpectedCount = UNEXPECTED_COUNT_PLACEHOLDER;

                            //String appName = resourceMetrics.getResource().getAttributesList();
                            String appName = getValueDataFromOptionalAnyValue(
                                    resourceMetrics.getResource().getAttributesList().stream()
                                            .filter(kv -> kv.getKey().equals("service.name"))
                                            .map(KeyValue::getValue)
                                            .findFirst(),
                                    logger
                            );

                            for (KeyValue attribute : dataPoint.getAttributesList()) {
                                if (attribute.getKey().equalsIgnoreCase(DATA_PRODUCT_NAME_KEY)) {
                                    dataProductName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                } else if (attribute.getKey().equalsIgnoreCase(EXPECTATION_NAME_KEY)) {
                                    expectationName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                } else if (attribute.getKey().equalsIgnoreCase(ELEMENT_COUNT_KEY)) {
                                    elementCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                } else if (attribute.getKey().equalsIgnoreCase(UNEXPECTED_COUNT_KEY)) {
                                    unexpectedCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                }
                                //else if (attribute.getKey().equalsIgnoreCase(APP_NAME_KEY)) {
                                //    appName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                //}
                            }

                            metricRepository.save(new Metric(
                                    dataProductName,
                                    appName,
                                    expectationName,
                                    currentMetric.getName(),
                                    currentMetric.getDescription(),
                                    dataPoint.getAsDouble() == 0.0 ? dataPoint.getAsInt() : dataPoint.getAsDouble(),
                                    currentMetric.getUnit(),
                                    Optional.ofNullable(elementCount)
                                            .map(Ints::tryParse)
                                            .orElse(0),
                                    Optional.ofNullable(unexpectedCount)
                                            .map(Ints::tryParse)
                                            .orElse(0),
                                    LocalDateTime.ofInstant(
                                                    Instant.ofEpochSecond(
                                                            dataPoint.getTimeUnixNano() / 1_000_000_000,
                                                            (int) (dataPoint.getTimeUnixNano() % 1_000_000_000)
                                                    ),
                                                    ZoneId.systemDefault()
                                            )
                                            .atZone(ZoneId.of("UTC"))
                                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"))
                            ));

                        });
                    } else if (currentMetric.getDataCase() == io.opentelemetry.proto.metrics.v1.Metric.DataCase.SUM) {
                        currentMetric.getSum().getDataPointsList().forEach(dataPoint -> {

                            String dataProductName = DATA_PRODUCT_NAME_PLACEHOLDER;
                            String expectationName = EXPECTATION_NAME_PLACEHOLDER;
                            String elementCount = ELEMENT_COUNT_PLACEHOLDER;
                            String unexpectedCount = UNEXPECTED_COUNT_PLACEHOLDER;

                            // String appName = resourceMetrics.getResource().getAttributesList();
                            String appName = getValueDataFromOptionalAnyValue(
                                    resourceMetrics.getResource().getAttributesList().stream()
                                        .filter(kv -> kv.getKey().equals("service.name"))
                                        .map(KeyValue::getValue)
                                        .findFirst(),
                                    logger
                            );

                            for (KeyValue attribute : dataPoint.getAttributesList()) {
                                if (attribute.getKey().equalsIgnoreCase(DATA_PRODUCT_NAME_KEY)) {
                                    dataProductName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                } else if (attribute.getKey().equalsIgnoreCase(EXPECTATION_NAME_KEY)) {
                                    expectationName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                } else if (attribute.getKey().equalsIgnoreCase(ELEMENT_COUNT_KEY)) {
                                    elementCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                } else if (attribute.getKey().equalsIgnoreCase(UNEXPECTED_COUNT_KEY)) {
                                    unexpectedCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                }
                                //else if (attribute.getKey().equalsIgnoreCase(APP_NAME_KEY)) {
                                //    appName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                //}
                            }

                            metricRepository.save(new Metric(
                                    dataProductName,
                                    appName,
                                    expectationName,
                                    currentMetric.getName(),
                                    currentMetric.getDescription(),
                                    dataPoint.getAsDouble() == 0.0 ? dataPoint.getAsInt() : dataPoint.getAsDouble(),
                                    currentMetric.getUnit(),
                                    Optional.ofNullable(elementCount)
                                            .map(Ints::tryParse)
                                            .orElse(0),
                                    Optional.ofNullable(unexpectedCount)
                                            .map(Ints::tryParse)
                                            .orElse(0),
                                    LocalDateTime.ofInstant(
                                                    Instant.ofEpochSecond(
                                                            dataPoint.getTimeUnixNano() / 1_000_000_000,
                                                            (int) (dataPoint.getTimeUnixNano() % 1_000_000_000)
                                                    ),
                                                    ZoneId.systemDefault()
                                            )
                                            .atZone(ZoneId.of("UTC"))
                                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"))
                            ));

                        });
                    }
                });
            });
        }
        responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
