package com.example.kafkabatchprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        // adicionar lógica antes do início do job se necessário
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus().isUnsuccessful()) {
            logger.error("Job failed with status: " + jobExecution.getStatus());
        } else {
            logger.info("Job completed successfully with status: " + jobExecution.getStatus());
        }
        // Fechar a aplicação após a conclusão do job
        System.exit(0);
    }
}
