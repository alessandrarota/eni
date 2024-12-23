package it.quantyca.OTELCustomCollector.service;

import io.grpc.stub.StreamObserver;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import it.quantyca.OTELCustomCollector.model.*;
import it.quantyca.OTELCustomCollector.repository.MetricRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.*;
import java.util.ArrayList;
import java.util.List;

import static it.quantyca.OTELCustomCollector.utility.Utils.getValueDataFromAnyValue;

@Service
public class OTELMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase {
    @Autowired
    private MetricRepository metricRepository;

    private Logger logger = LoggerFactory.getLogger(OTELMetricsService.class);

    @Value("${otlp.metric.dataProductName.placeholder:N/A}")
    private String DATA_PRODUCT_NAME_PLACEHOLDER;
    @Value("${otlp.metric.dataProductName.key:data-product-name}")
    private String DATA_PRODUCT_NAME_KEY;
    @Value("${otlp.metric.appName.placeholder:N/A}")
    private String APP_NAME_PLACEHOLDER;
    @Value("${otlp.metric.appName.key:app-name}")
    private String APP_NAME_KEY;


    @Override
    @Transactional
    public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        logger.info("Processing new METRIC");
        logger.info(request.toString());

        // for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
        //     resourceMetrics.getScopeMetricsList().forEach(scopeMetric -> {
        //         scopeMetric.getMetricsList().forEach(currentMetric -> {
        //             if (currentMetric.getDataCase() == io.opentelemetry.proto.metrics.v1.Metric.DataCase.GAUGE) {
        //                 currentMetric.getGauge().getDataPointsList().forEach(dataPoint -> {

        //                     String dataProductName = DATA_PRODUCT_NAME_PLACEHOLDER;
        //                     String appName = APP_NAME_PLACEHOLDER;

        //                     for (KeyValue attribute : dataPoint.getAttributesList()) {
        //                         if (attribute.getKey().equalsIgnoreCase(DATA_PRODUCT_NAME_KEY)) {
        //                             dataProductName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
        //                         } else if (attribute.getKey().equalsIgnoreCase(APP_NAME_KEY)) {
        //                             appName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
        //                         }
        //                     }

        //                     Metric newMetric = new Metric(
        //                             dataProductName,
        //                             appName,
        //                             currentMetric.getName(),
        //                             currentMetric.getDescription(),
        //                             dataPoint.getAsDouble() == 0.0 ? dataPoint.getAsInt() : dataPoint.getAsDouble(),
        //                             currentMetric.getUnit(),
        //                             ZonedDateTime.ofInstant(
        //                                     Instant.ofEpochSecond(
        //                                             dataPoint.getTimeUnixNano() / 1_000_000_000,
        //                                             (int) (dataPoint.getTimeUnixNano() % 1_000_000_000)
        //                                     ),
        //                                     ZoneOffset.UTC // Usa UTC per il fuso orario
        //                             ).toString()
        //                     );
        //                     System.out.println(newMetric);
        //                     metricRepository.save(newMetric);

        //                 });
        //             } else if (currentMetric.getDataCase() == io.opentelemetry.proto.metrics.v1.Metric.DataCase.SUM) {
        //                 currentMetric.getSum().getDataPointsList().forEach(dataPoint -> {

        //                     String dataProductName = DATA_PRODUCT_NAME_PLACEHOLDER;
        //                     String appName = APP_NAME_PLACEHOLDER;

        //                     for (KeyValue attribute : dataPoint.getAttributesList()) {
        //                         if (attribute.getKey().equalsIgnoreCase(DATA_PRODUCT_NAME_KEY)) {
        //                             dataProductName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
        //                         } else if (attribute.getKey().equalsIgnoreCase(APP_NAME_KEY)) {
        //                             appName = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
        //                         }
        //                     }

        //                     Metric newMetric = new Metric(
        //                             dataProductName,
        //                             appName,
        //                             currentMetric.getName(),
        //                             currentMetric.getDescription(),
        //                             dataPoint.getAsDouble() == 0.0 ? dataPoint.getAsInt() : dataPoint.getAsDouble(),
        //                             currentMetric.getUnit(),
        //                             ZonedDateTime.ofInstant(
        //                                     Instant.ofEpochSecond(
        //                                             dataPoint.getTimeUnixNano() / 1_000_000_000,
        //                                             (int) (dataPoint.getTimeUnixNano() % 1_000_000_000)
        //                                     ),
        //                                     ZoneOffset.UTC // Usa UTC per il fuso orario
        //                             ).toString()
        //                     );
        //                     System.out.println(newMetric);
        //                     metricRepository.save(newMetric);

        //                 });
        //             }
        //         });
        //     });
        // }
        responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
