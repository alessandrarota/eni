package it.quantyca.OTELCustomCollector.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity(name = "Log")
@Table(name = "log")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Log {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long ID;
    private LocalDateTime timestamp;
    private String severityLevel;
    private String sourceFile;
    private String sourceFunction;
    private String lineNumber;
    private String message;

    public Log(LocalDateTime timestamp, String severityLevel, String sourceFile, String sourceFunction, String lineNumber, String message) {
        this.timestamp = timestamp;
        this.severityLevel = severityLevel;
        this.sourceFile = sourceFile;
        this.sourceFunction = sourceFunction;
        this.lineNumber = lineNumber;
        this.message = message;
    }

    @Override
    public String toString() {
        return String.format(
                "Log[timestamp='%s', severityLevel='%s', source='%s %s', lineNumber=%s, message='%s']",
                timestamp.toString(), severityLevel, sourceFile, sourceFunction, lineNumber, message
        );
    }
}
