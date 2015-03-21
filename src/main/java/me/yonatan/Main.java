package me.yonatan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import me.yonatan.model.BitcasaFile;
import me.yonatan.service.FileFetcher;
import me.yonatan.service.ListFetcher;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yonatan on 12/1/14.
 */
@SpringBootApplication
@Slf4j
public class Main implements CommandLineRunner {
    public static void main(String... args) {
        try {
            SpringApplication.run(Main.class, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Autowired
    ListFetcher listFetcher;

    @Autowired
    FileFetcher fileFetcher;

    @Autowired
    ObjectMapper objectMapper;


    @Value("${bitscape.sendId}")
    private String sendId;

    @Value("${bitscape.destFolder}")
    private String dst;

    @Override
    public void run(String... strings) throws Exception {
        log.info("Fetching files from sendId {} to folder {}", sendId, dst);
        List<BitcasaFile> files = listFetcher.getFiles(sendId);
        log.info("Received {} files to download. Starting to download.", files.size());
        fileFetcher.downloadFiles(dst, files);
    }
}
