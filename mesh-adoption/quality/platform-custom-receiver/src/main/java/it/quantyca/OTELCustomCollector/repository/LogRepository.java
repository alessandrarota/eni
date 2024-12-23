package it.quantyca.OTELCustomCollector.repository;

import it.quantyca.OTELCustomCollector.model.Log;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LogRepository extends JpaRepository<Log, Long> {
}
