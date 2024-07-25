package com.example.kafkabatchprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
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

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("file-to-kafka-step")
                .<String, String>chunk(100)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();

        return jobBuilderFactory.get("file-to-kafka-job")
                .start(step)
                .build();
    }

    @Bean
    public FlatFileItemReader<String> reader() {
        String filePath = "/app/input/output.txt";
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
        return item -> {
            //logger.info("Processing item: {}", item);
            return item; // No processing required
        };
    }

    @Bean
    public ItemWriter<String> writer() {
        return new KafkaItemWriter();
    }
}
