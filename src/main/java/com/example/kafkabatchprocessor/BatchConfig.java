package com.santander.kafkabatchprocessor;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Slf4j
@Configuration
public class BatchConfig {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    @Autowired
    public JobCompletionNotificationListener listener;

    @Bean
    public Job job(Step step,
                   JobRepository jobRepository) {
        return new JobBuilder("file-to-kafka-job", jobRepository)
                .start(step)
                .listener(listener)
                .build();
    }

    @Bean
    public Step step(FlatFileItemReader<String> reader,
                     ItemWriter<String> writer,
                     DataSourceTransactionManager transactionManager,
                     JobRepository jobRepository) throws Exception {

        log.info("Creating step...");

        return new StepBuilder("file-to-kafka-step", jobRepository)
                .<String, String>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor())
                .writer(writer)
                .build();
    }

    @Bean
    public FlatFileItemReader<String> reader(@Value("${input.file.path}") String filePath) {
        logger.info("Reading file from path: {}", filePath);

        FileSystemResource resource = new FileSystemResource(filePath);
        if (resource.exists()) {
            logger.info("File exists: {}", filePath);
        } else {
            logger.error("File does not exist: {}", filePath);
        }

        return new FlatFileItemReaderBuilder<String>()
                .name("lineItemReader")
                .resource(resource)
                .lineMapper(new PassThroughLineMapper())
                .build();
    }

    @Bean
    public ItemProcessor<String, String> processor() {
        return item -> item; // No processing required
    }
 
    @Bean
    public ItemWriter<String> writer() {
        return new KafkaItemWriter();
    }
}
