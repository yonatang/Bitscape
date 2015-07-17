package me.yonatan.bitscape.service.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import me.yonatan.bitscape.model.BitcasaFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by yonatan on 10/7/2015.
 */
@Component
@Slf4j
@StepScope
public class BitcasaFileListReader implements ItemReader<BitcasaFile> {
    @Autowired
    private ObjectMapper objectMapper;

    @Value("#{jobParameters['sendId']}")
    private String sendId;

    @Value("${bitscape.thread.poolSize:10}")
    private int poolSize;

    private ExecutorService executor;

    @PostConstruct
    public void init() throws Exception {
        log.info("Getting files for sendId {}", getPartialSendId());
        executor = Executors.newFixedThreadPool(poolSize);
        filesIter = getFiles(sendId).iterator();
    }

    private String getMetaJson(String url) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault();) {
            HttpGet get = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(get);) {
                return IOUtils.toString(response.getEntity().getContent());
            }
        }
    }

    private BitcasaFile parseData(JsonNode node) {
        BitcasaFile file = new BitcasaFile();
        file.setId(node.get("id").asText());
        file.setName(node.get("name").asText());
        file.setType(node.get("type").asText());
        if (file.getType().equals("file")) {
            file.setSize(node.get("size").asLong());
            JsonNode nebula = node.get("application_data").get("_server").get("nebula");
            if (nebula == null) {
                return null;
            }
            file.setPayload(nebula.get("payload").asText());
            file.setDigest(nebula.get("digest").asText());
            file.setNonce(nebula.get("nonce").asText());
        }
        return file;
    }

    public List<BitcasaFile> getFiles(String sendId) throws IOException, InterruptedException, ExecutionException {
        Set<Future<Void>> futures = Collections.newSetFromMap(new ConcurrentHashMap<Future<Void>, Boolean>());
        List<BitcasaFile> files = Collections.synchronizedList(new ArrayList<BitcasaFile>());
        FileGetters fileGetters = new FileGetters(futures, files, sendId, "");
        Future<Void> submit = executor.submit(fileGetters);
        futures.add(submit);

        while (true) {
            boolean allAreDone = true;
            Set<Future<Void>> futuresCopy = new HashSet<>(futures);
            for (Future<Void> voidFuture : futuresCopy) {
                if (!voidFuture.isDone()) {
                    allAreDone = false;
                }
                voidFuture.get();
            }
            if (allAreDone) {
                break;
            }
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        Collections.sort(files, new Comparator<BitcasaFile>() {
            @Override
            public int compare(BitcasaFile o1, BitcasaFile o2) {
                return (int)(o1.getSize()-o2.getSize());
            }
        });
        return files;
    }

    class FileGetters implements Callable<Void> {
        private final String path;
        private final String sendId;
        private final List<BitcasaFile> files;
        private final Set<Future<Void>> futures;

        public FileGetters(Set<Future<Void>> futures,
                           List<BitcasaFile> files, String sendId, String path) {
            this.sendId = sendId;
            this.path = path;
            this.files = files;
            this.futures = futures;
        }

        @Override
        public Void call() throws Exception {
            log.debug("Fetching files for {}", path);
            String shareUrl = "https://drive.bitcasa.com/portal/v2/shares/" + sendId + "/meta";
            String jsonString = getMetaJson(shareUrl);
            JsonNode jsonNode = objectMapper.reader().readTree(jsonString);
            if (jsonNode.get("result").isNull()) {
                String errorMessage = jsonNode.get("error").get("message").asText();
                log.error("Error while fetching list from {}:\n{}", shareUrl, errorMessage);
                throw new RuntimeException(errorMessage);
            }
            ArrayNode items = (ArrayNode) jsonNode.get("result").get("items");
//            List<BitcasaFile> result = new ArrayList<>();
            for (JsonNode item : items) {
                BitcasaFile file = parseData(item);
                if (file == null) {
                    continue;
                }
                if (file.getType().equals("folder")) {
                    Future<Void> submit = executor.submit(
                            new FileGetters(futures, files, sendId + "/" + file.getId(), path + "/" + file.getName()));
                    futures.add(submit);
                } else {
                    file.setPath(path);
                    files.add(file);
                }
            }
            return null;
        }
    }

    private String getPartialSendId() {
        return StringUtils.substring(sendId, 0, 4) + "..." + StringUtils.substring(sendId, -4);
    }

    private Iterator<BitcasaFile> filesIter;

    @Override
    public BitcasaFile read() {
        if (filesIter.hasNext()) {
            return filesIter.next();
        }
        return null;
    }
}
