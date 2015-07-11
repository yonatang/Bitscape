package me.yonatan.bitscape.config;

import lombok.extern.slf4j.Slf4j;
import me.yonatan.bitscape.model.BitcasaFile;
import me.yonatan.bitscape.service.jobs.BitcasaDownloader;
import me.yonatan.bitscape.service.jobs.BitcasaFileException;
import me.yonatan.bitscape.service.jobs.BitcasaFileListReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Created by yonatan on 10/7/2015.
 */
@Configuration
@EnableBatchProcessing
@Slf4j
public class BatchConfig {

    @Bean
    public Job bitcasaMigrationJob(JobBuilderFactory jobs, Step s1) {
        return jobs.get("bitcasaMigrationJob")
                .incrementer(new RunIdIncrementer())

                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {

                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        log.info("Job has completed");
                    }
                })
                .flow(s1)
                .end()
                .build();
    }

    @Bean
    public JobLauncher asyncJobLauncher(JobRepository jobRepository, SimpleAsyncTaskExecutor simpleAsyncTaskExecutor) {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(simpleAsyncTaskExecutor);
        return simpleJobLauncher;
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory,
                      SimpleAsyncTaskExecutor simpleAsyncTaskExecutor,
                      BitcasaFileListReader reader,
                      BitcasaDownloader bitcasaDownloader) {
        return stepBuilderFactory.get("step1")
                .<BitcasaFile, Void>chunk(10)
                .reader(reader)
                .processor(bitcasaDownloader)
                .faultTolerant()
                .skip(BitcasaFileException.class)
                .skipLimit(Integer.MAX_VALUE)
                .noSkip(RuntimeException.class)
                .taskExecutor(simpleAsyncTaskExecutor)
                .build();

    }

}
