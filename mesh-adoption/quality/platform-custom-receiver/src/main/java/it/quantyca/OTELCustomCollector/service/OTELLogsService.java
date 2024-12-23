package it.quantyca.OTELCustomCollector.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import it.quantyca.OTELCustomCollector.model.Log;
import it.quantyca.OTELCustomCollector.repository.LogRepository;
import static it.quantyca.OTELCustomCollector.utility.Utils.getValueDataFromAnyValue;
import jakarta.transaction.Transactional;

@Service
public class OTELLogsService extends LogsServiceGrpc.LogsServiceImplBase {
    @Autowired
    private LogRepository logRepository;

    private Logger logger = LoggerFactory.getLogger(OTELLogsService.class);

    @Value("${oltp.log.sourceFile.key:code.filepath}")
    private String SOURCE_FILE_KEY;
    @Value("${oltp.log.sourceFile.placeholder:N/A}")
    private String SOURCE_FILE_PLACEHOLDER;
    @Value("${oltp.log.sourceFunction.key:code.function}")
    private String SOURCE_FUNCTION_KEY;
    @Value("${oltp.log.sourceFunction.placeholder:N/A}")
    private String SOURCE_FUNCTION_PLACEHOLDER;
    @Value("${oltp.log.lineNumber.key:code.lineno}")
    private String LINE_NUMBER_KEY;
    @Value("${oltp.log.lineNumber.placeholder:-1}")
    private String LINE_NUMBER_PLACEHOLDER;


    @Override
    @Transactional
    public void export(ExportLogsServiceRequest request, StreamObserver<ExportLogsServiceResponse> responseObserver) {
        logger.info("Processing new LOG");
        logger.debug(request.toString());

        for (ResourceLogs resourceLogs : request.getResourceLogsList()) {
            resourceLogs.getScopeLogsList().forEach(
                    scopeLog -> {
                        scopeLog.getLogRecordsList().forEach(
                                logRecord -> {
                                    String sourceFile = SOURCE_FILE_PLACEHOLDER;
                                    String sourceFunction = SOURCE_FUNCTION_PLACEHOLDER;
                                    String lineNumber = LINE_NUMBER_PLACEHOLDER;

                                    for (KeyValue attribute : logRecord.getAttributesList()) {
                                        if (attribute.getKey().equalsIgnoreCase(SOURCE_FILE_KEY)) {
                                            sourceFile = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                        } else if (attribute.getKey().equalsIgnoreCase(SOURCE_FUNCTION_KEY)) {
                                            sourceFunction = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                        } else if (attribute.getKey().equalsIgnoreCase(LINE_NUMBER_KEY)) {
                                            lineNumber = getValueDataFromAnyValue(attribute.getValue(), logger).strip();
                                        }
                                    }

                                    logRepository.save(new Log(
                                            LocalDateTime.ofInstant(
                                                    Instant.ofEpochSecond(
                                                            logRecord.getTimeUnixNano() / 1_000_000_000,
                                                            (int) (logRecord.getTimeUnixNano() % 1_000_000_000)
                                                    ), ZoneId.systemDefault()
                                            ),
                                            logRecord.getSeverityText(),
                                            sourceFile,
                                            sourceFunction,
                                            lineNumber,
                                            logRecord.getBody().getStringValue()
                                    ));
                                }
                        );
                    }
            );
        }
        responseObserver.onNext(ExportLogsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
