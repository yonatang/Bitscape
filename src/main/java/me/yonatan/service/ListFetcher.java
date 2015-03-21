package me.yonatan.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import me.yonatan.model.BitcasaFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

/**
 * Created by yonatan on 12/1/14.
 */
@Service
@Slf4j
public class ListFetcher {
    //Bitcasa's proprietary voodoo REST data structure
    @Autowired
    private ObjectMapper objectMapper;

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

    public List<BitcasaFile> getFiles(String sendId) throws IOException {
        return getFiles(sendId, "");
    }

    public List<BitcasaFile> getFiles(String sendId, String path) throws IOException {
        String shareUrl = "https://drive.bitcasa.com/portal/v2/shares/" + sendId + "/meta";
        String jsonString = getMetaJson(shareUrl);
        JsonNode jsonNode = objectMapper.reader().readTree(jsonString);
        if (jsonNode.get("result").isNull()) {
            String errorMessage = jsonNode.get("error").get("message").asText();
            log.error("Error while fetching list from {}:\n{}", shareUrl, errorMessage);
            throw new RuntimeException(errorMessage);
        }
        ArrayNode items = (ArrayNode) jsonNode.get("result").get("items");
        List<BitcasaFile> result = new ArrayList<>();
        for (JsonNode item : items) {
            BitcasaFile file = parseData(item);
            if (file == null) {
                continue;
            }
            if (file.getType().equals("folder")) {
                List<BitcasaFile> recursive = getFiles(sendId + "/" + file.getId(), path + "/" + file.getName());
                result.addAll(recursive);
            } else {
                file.setPath(path);
                result.add(file);
            }
        }
        return result;
    }
}
