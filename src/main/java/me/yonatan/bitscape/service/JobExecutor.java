package me.yonatan.bitscape.service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * Created by yonatan on 10/7/2015.
 */
@Service
public class JobExecutor {

    @Autowired
    private Job migrateFilesJob;

    @Autowired
    private JobLauncher asyncJobLauncher;

    public JobExecution startMigration(String sendId, String awsKey, String awsSecret, String bucket) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("date", new Date())
                .addString("sendId", sendId)
                .addString("awsKey", awsKey)
                .addString("awsSecret", awsSecret)
                .addString("s3Bucket", bucket)
                .toJobParameters();

        return asyncJobLauncher.run(migrateFilesJob, jobParameters);
    }
}
