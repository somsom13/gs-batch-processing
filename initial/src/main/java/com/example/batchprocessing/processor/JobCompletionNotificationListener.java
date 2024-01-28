package com.example.batchprocessing.processor;

import com.example.batchprocessing.domain.Person;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

// job이 특정 상태가 되었을 때를 캐치하고, 그 전이나 후에 처리할 작업을 정의할 수 있다.
@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private final JdbcTemplate jdbcTemplate;

    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) { // job의 상태가 종료일 때를 캐치한다.
            System.out.println("JOB FINISHED!! Verifying results");

            jdbcTemplate.query("SELECT first_name, last_name FROM people",
                    new DataClassRowMapper<>(Person.class))
                .forEach(person ->
                    System.out.printf("found, %s %s%n", person.firstName(), person.lastName())
                );
        }
    }
}
