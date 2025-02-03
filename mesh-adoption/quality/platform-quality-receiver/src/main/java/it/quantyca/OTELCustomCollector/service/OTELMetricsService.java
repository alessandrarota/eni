package it.quantyca.OTELCustomCollector.service;

import io.grpc.stub.StreamObserver;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import it.quantyca.OTELCustomCollector.model.*;
import it.quantyca.OTELCustomCollector.repository.BusinessDomainRepository;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static it.quantyca.OTELCustomCollector.utility.Utils.*;

@Service
public class OTELMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase {
    @Autowired
    private MetricRepository metricRepository;
    @Autowired
    private BusinessDomainRepository businessDomainRepository;

    private Logger logger = LoggerFactory.getLogger(OTELMetricsService.class);

    @Value("${otlp.metric.signalType.key:signal_type}")
    private String SIGNAL_TYPE_KEY;
    @Value("${otlp.metric.signalType.filterValue:DATA_QUALITY}")
    private String SIGNAL_TYPE_FILTER_VALUE;

    @Value("${otlp.metric.businessDomainName.key:business_domain_name}")
    private String BUSINESS_DOMAIN_NAME_KEY;
    @Value("${otlp.metric.dataProductName.key:data_product_name}")
    private String DATA_PRODUCT_NAME_KEY;

    @Value("${otlp.metric.dataSourceName.key:data_source_name}")
    private String DATA_SOURCE_NAME_KEY;
    @Value("${otlp.metric.dataAssetName.key:data_asset_name}")
    private String DATA_ASSET_NAME_KEY;
    @Value("${otlp.metric.columnName.key:column_name}")
    private String COLUMN_NAME_KEY;

    @Value("${otlp.metric.expectationName.key:expectation_name}")
    private String EXPECTATION_NAME_KEY;
    @Value("${otlp.metric.expectationName.placeholder:N/A}")
    private String EXPECTATION_NAME_PLACEHOLDER;
    @Value("${otlp.metric.gxSuiteName.key:gx_suite_name}")
    private String GX_SUITE_NAME_KEY;
    @Value("${otlp.metric.gxSuiteName.placeholder:N/A}")
    private String GX_SUITE_NAME_PLACEHOLDER;
    @Value("${otlp.metric.elementCount.key:element_count}")
    private String ELEMENT_COUNT_KEY;
    @Value("${otlp.metric.elementCount.placeholder:0}")
    private String ELEMENT_COUNT_PLACEHOLDER;
    @Value("${otlp.metric.unexpectedCount.key:unexpected_count}")
    private String UNEXPECTED_COUNT_KEY;
    @Value("${otlp.metric.unexpectedCount.placeholder:0}")
    private String UNEXPECTED_COUNT_PLACEHOLDER;
    @Value("${otlp.metric.appName.key:app-name}")
    private String APP_NAME_KEY;
    @Value("${otlp.metric.appName.placeholder:N/A}")
    private String APP_NAME_PLACEHOLDER;
    @Value("${otlp.metric.dataQualityDimensionName.key:data_quality_dimension_name}")
    private String DATA_QUALITY_DIMENSION_NAME_KEY;
    @Value("${otlp.metric.dataQualityDimensionName.placeholder:N/A}")
    private String DATA_QUALITY_DIMENSION_NAME_PLACEHOLDER;

    @Value("${otlp.metric.status_value:NEW}")
    private String STATUS_VALUE;


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
                        logger.warn("Wasting metric... Current DataCase (" +
                                currentMetric.getDataCase() + ") not managed.");
                        dataPoints = new ArrayList<NumberDataPoint>();
                    }

                    dataPoints.forEach(dataPoint -> {
                        String signalTypeValue = "";

                        String originalBusinessDomainName = null, reconductedBusinessDomainName = null;
                        String originalDataProductName = "", cleanDataProductName = "";

                        String expectationName = EXPECTATION_NAME_PLACEHOLDER;
                        String gxSuiteName = GX_SUITE_NAME_PLACEHOLDER;
                        String elementCount = ELEMENT_COUNT_PLACEHOLDER;
                        String unexpectedCount = UNEXPECTED_COUNT_PLACEHOLDER;
                        String dataQualityDimensionName = DATA_QUALITY_DIMENSION_NAME_PLACEHOLDER;

                        String attr_dataSourceName = null;
                        String attr_dataAssetName = null;
                        String attr_columnName = null;

                        String appName = getValueDataFromOptionalAnyValue(
                                resourceMetrics.getResource().getAttributesList().stream()
                                        .filter(kv -> kv.getKey().equals("service.name"))
                                        .map(KeyValue::getValue)
                                        .findFirst(),
                                logger
                        );

                        for (KeyValue attribute : dataPoint.getAttributesList()) {
                            if (attribute.getKey().equalsIgnoreCase(SIGNAL_TYPE_KEY)) {
                                signalTypeValue = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(BUSINESS_DOMAIN_NAME_KEY)) {
                                originalBusinessDomainName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(DATA_PRODUCT_NAME_KEY)) {
                                originalDataProductName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(EXPECTATION_NAME_KEY)) {
                                expectationName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(GX_SUITE_NAME_KEY)) {
                                gxSuiteName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(ELEMENT_COUNT_KEY)) {
                                elementCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(UNEXPECTED_COUNT_KEY)) {
                                unexpectedCount = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(DATA_QUALITY_DIMENSION_NAME_KEY)) {
                                dataQualityDimensionName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(DATA_SOURCE_NAME_KEY)) {
                                attr_dataSourceName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(DATA_ASSET_NAME_KEY)) {
                                attr_dataAssetName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            } else if (attribute.getKey().equalsIgnoreCase(COLUMN_NAME_KEY)) {
                                attr_columnName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                            }
                        }

                        if (! signalTypeValue.equalsIgnoreCase(SIGNAL_TYPE_FILTER_VALUE)) {
                            logger.warn("Wasting metric... " +
                                    "Signal type expected: [" + SIGNAL_TYPE_FILTER_VALUE + "] " +
                                    "Signal type received: [" + signalTypeValue + "]");
                        } else if (attr_dataSourceName == null || attr_dataSourceName.isEmpty()) {
                            logger.warn("Wasting metric... " + DATA_SOURCE_NAME_KEY + " attribute must be valued.");
                        } else if (attr_dataAssetName == null || attr_dataAssetName.isEmpty()) {
                            logger.warn("Wasting metric... " + DATA_ASSET_NAME_KEY + " attribute must be valued.");
                        } else if (attr_columnName == null || attr_columnName.isEmpty()) {
                            logger.warn("Wasting metric... " + COLUMN_NAME_KEY + " attribute must be valued.");
                        } else {

                            reconductedBusinessDomainName = reconductBusinessDomain(originalBusinessDomainName, businessDomainRepository.getBusinessDomains());
                            cleanDataProductName = cleanStringCamelCase(originalDataProductName);

                            logger.warn("Storing new metric data point.");
                            metricRepository.save(new Metric(
                                    reconductedBusinessDomainName == null ? originalBusinessDomainName : reconductedBusinessDomainName,
                                    cleanDataProductName,
                                    expectationName,
                                    attr_dataSourceName,
                                    attr_dataAssetName,
                                    attr_columnName,
                                    reconductedBusinessDomainName == null ? null : reconductedBusinessDomainName + "-" + cleanDataProductName,
                                    gxSuiteName,
                                    dataQualityDimensionName,
                                    dataPoint.getAsDouble() == 0.0 ? dataPoint.getAsInt() : dataPoint.getAsDouble(),
                                    currentMetric.getUnit(),
                                    Optional.ofNullable(elementCount).map(Ints::tryParse).orElse(0),
                                    Optional.ofNullable(unexpectedCount).map(Ints::tryParse).orElse(0),
                                    appName,
                                    LocalDateTime.ofInstant(
                                            Instant.ofEpochSecond(
                                                    dataPoint.getTimeUnixNano() / 1_000_000_000,
                                                    (int) (dataPoint.getTimeUnixNano() % 1_000_000_000)
                                            ),
                                            ZoneId.systemDefault()
                                    ),
                                    STATUS_VALUE,
                                    null,
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