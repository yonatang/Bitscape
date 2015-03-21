package me.yonatan.service;

import lombok.extern.slf4j.Slf4j;
import me.yonatan.model.BitcasaFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by yonatan on 12/1/14.
 */
@Service
@Slf4j
public class FileFetcher {
    @Value("${bitscape.thread.poolSize:5}")
    private int poolSize;

    @Value("${bitscape.domain:bitcasa.cfsusercontent.io}")
    private String domain;

    ExecutorService executor;

    @PostConstruct
    private void init() {
        log.debug("Using thread pool of size {}", poolSize);
        log.debug("Fetching from bitcasa domain {}", domain);
        executor = Executors.newFixedThreadPool(poolSize);
    }

    public String fileUrl(String contentDomain, BitcasaFile file) {
        String request = StringUtils.join(Arrays.asList("download", "v2", file.getDigest(), file.getNonce(),
                file.getPayload()), "/");
        return "https://" + contentDomain + "/" + request;
    }

    class BitcasaFileException extends Exception {
        BitcasaFile bitcasaFile;

        BitcasaFileException(BitcasaFile file, String message, Exception e) {
            super(message, e);
            this.bitcasaFile = file;
        }
    }

    public void downloadFiles(String targetDirectory, List<BitcasaFile> files) throws ExecutionException, InterruptedException {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(poolSize + 2);
        final CloseableHttpClient client = HttpClients.createMinimal(cm);

        List<Future<Void>> futures = new ArrayList<>();
        File dir = new File(targetDirectory);
        if (!dir.exists()) {
            //todo better handling here
            dir.mkdirs();
        }
        for (final BitcasaFile bitcasaFile : files) {
            File targetDir = new File(dir, bitcasaFile.getPath());
            if (!targetDir.exists()) {
                targetDir.mkdirs();
            }
            final File target = new File(targetDir, bitcasaFile.getName());
            final File tmpTarget = new File(target.getAbsolutePath() + ".part");
            tmpTarget.delete();

            if (target.exists()) {
                log.info("File {} exists. Existing file size: {}, Bitcasa size: {}", bitcasaFile.getName(), target.length(), bitcasaFile.getSize());
                if (target.length() != bitcasaFile.getSize()) {
                    log.warn("File {} wasn't saved properly in the last session. Deleting and re-downloading.", bitcasaFile.getName());
                    target.delete();
                } else {
                    continue;
                }
            }
            try {
                FileUtils.touch(tmpTarget);
            } catch (IOException e) {
            }
            final HttpGet get = new HttpGet(fileUrl(domain, bitcasaFile));
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    log.info("Downloading file {}", bitcasaFile.getName());
                    try (CloseableHttpResponse response = client.execute(get);) {
                        try (InputStream is = response.getEntity().getContent()) {
                            log.debug("Saving {} as temp file {}", bitcasaFile.getName(), tmpTarget);
                            FileUtils.copyInputStreamToFile(is, tmpTarget);
                            log.info("File {} saved. File size: {}", tmpTarget.length());
                            if (tmpTarget.length() != bitcasaFile.getSize()) {
                                log.error("File {} wasn't downloaded properly!", bitcasaFile.getName());
                            } else {
                                tmpTarget.renameTo(target);
                            }
                        }
                        return null;
                    } catch (Exception e) {
                        throw new BitcasaFileException(bitcasaFile, e.getMessage(), e);
                    }
                }
            }));
        }


        List<Pair<BitcasaFile, String>> failedFiles = new ArrayList<>();
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                StringBuilder sb = new StringBuilder("Error while retrieving file: ");
                if (e.getCause() instanceof BitcasaFileException) {
                    BitcasaFileException bfe = (BitcasaFileException) e.getCause();
                    BitcasaFile bf = bfe.bitcasaFile;
                    Pair<BitcasaFile, String> pair = ImmutablePair.of(bf, bfe.getMessage());
                    failedFiles.add(pair);
                    sb.append(bf.getPath()).append('/').append(bf.getName()).append(' ');
                }
                sb.append(e.getMessage());
                log.error(sb.toString());
            }
        }
        log.info("Download done");
        if (failedFiles.size() > 0) {
            log.warn("Failed downloads:");
            List<String> lines = new ArrayList<>();
            for (Pair<BitcasaFile, String> failedFile : failedFiles) {
                BitcasaFile bf = failedFile.getLeft();
                String line = bf.getPath() + "/" + bf.getName() + "\t\t- " + failedFile.getRight();
                log.warn(line);
                lines.add(line);
            }
            try {
                File f = new File("report.txt");
                FileUtils.writeLines(f, lines, true);
                log.info("Failed files written to {}", f);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
    }

}
