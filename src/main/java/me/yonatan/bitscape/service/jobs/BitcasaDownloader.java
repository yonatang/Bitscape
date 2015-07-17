package me.yonatan.bitscape.service.jobs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import me.yonatan.bitscape.model.BitcasaFile;
import me.yonatan.bitscape.model.ConcreteBitcasaFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Created by yonatan on 11/7/2015.
 */
@StepScope
@Component
@Slf4j
public class BitcasaDownloader implements ItemProcessor<BitcasaFile, Void> {
    @Value("${bitscape.tempdir:/tmp/bitscape}")
    private String targetDirectory;

    @Value("${bitscape.thread.poolSize:10}")
    private int poolSize;

    @Value("${bitscape.domain:bitcasa.cfsusercontent.io}")
    private String domain;

    @Value("#{jobParameters['s3Bucket']}")
    private String s3Bucket;

    @Value("#{jobParameters['awsKey']}")
    private String awsKey;

    @Value("#{jobParameters['awsSecret']}")
    private String awsSecret;

    private AmazonS3Client amazonS3Client;

    public String fileUrl(String contentDomain, BitcasaFile file) {
        String request = StringUtils.join(Arrays.asList("download", "v2", file.getDigest(), file.getNonce(),
                file.getPayload()), "/");
        return "https://" + contentDomain + "/" + request;
    }

    private CloseableHttpClient client;

    @PostConstruct
    public void init() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(poolSize + 2);
        client = HttpClients.createMinimal(cm);
        AWSCredentials credentials = new BasicAWSCredentials(awsKey, awsSecret);
        amazonS3Client = new AmazonS3Client(credentials);
    }

    private String getObjectKey(BitcasaFile bitcasaFile) {
        String objectKey = bitcasaFile.getPath() + "/" + bitcasaFile.getName();
        return StringUtils.removeStart(objectKey, "/");
    }

    private void s3Upload(ConcreteBitcasaFile bitcasaFile) {
        log.info("Uploading to {} the file {}", s3Bucket, bitcasaFile);
        String objectKey = getObjectKey(bitcasaFile);

        PutObjectRequest putObjectRequest = new PutObjectRequest(s3Bucket, objectKey, bitcasaFile.getDownloadedFile());
        putObjectRequest.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl);
        amazonS3Client.putObject(putObjectRequest);
        log.info("File {} was created", objectKey);
    }

    private boolean isFileExistInS3(BitcasaFile bitcasaFile) {
        String objectKey = getObjectKey(bitcasaFile);
        ObjectListing objectListing = amazonS3Client.listObjects(s3Bucket, objectKey);
        for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
            if (s3ObjectSummary.getKey().equals(objectKey)) {
                return s3ObjectSummary.getSize() == bitcasaFile.getSize();
            }
        }
        return false;
    }

    @Override
    public Void process(BitcasaFile bitcasaFile) throws Exception {
        if (isFileExistInS3(bitcasaFile)) {
            log.info("Skipping on file {} {} - exists in S3 as {}", bitcasaFile.getPath(), bitcasaFile.getName(),
                    getObjectKey(bitcasaFile));
            return null;
        }
        File dir = new File(targetDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File targetDir = new File(dir, bitcasaFile.getPath());
        File target = new File(targetDir, bitcasaFile.getName());
        try {
            targetDir.mkdirs();
            target.delete();
            HttpGet get = new HttpGet(fileUrl(domain, bitcasaFile));

            log.info("Downloading {} bytes of file {} {}", bitcasaFile.getSize(), bitcasaFile.getPath(), bitcasaFile.getName());
            try (CloseableHttpResponse response = client.execute(get);) {
                try (InputStream is = response.getEntity().getContent()) {
                    log.debug("Saving {} as temp file {}", bitcasaFile.getName(), target);
                    FileUtils.copyInputStreamToFile(is, target);
                    log.info("File {} saved. File size: {}", target, target.length());
                    if (target.length() != bitcasaFile.getSize()) {
                        log.error("File {} {} wasn't downloaded properly!", bitcasaFile.getPath(), bitcasaFile.getName());
                        throw new BitcasaFileException(bitcasaFile, "File was not downloaded properly");
                    }
                }
            } catch (Exception e) {
                throw new BitcasaFileException(bitcasaFile, e.getMessage(), e);
            }
            ConcreteBitcasaFile concreteBitcasaFile = new ConcreteBitcasaFile(bitcasaFile, target);
            s3Upload(concreteBitcasaFile);
        } finally {
            log.debug("Deleting file {}", target);
            FileUtils.deleteQuietly(target);
        }
        return null;
    }

}
