package me.yonatan.bitscape;

import lombok.extern.slf4j.Slf4j;
import me.yonatan.bitscape.service.JobExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@SpringBootApplication
@Slf4j
public class BitscapeApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(BitscapeApplication.class, args);
    }

    @Autowired
    private JobExecutor jobExecutor;

    @Override
    public void run(String... args) throws Exception {
        System.out.print("Enter the send id: ");
        BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
        String sendId = bufferRead.readLine();
        System.out.print("\nEnter the destination bucket: ");
        String bucket = bufferRead.readLine();
        System.out.print("\nEnter the destination aws key: ");
        String awsKey = bufferRead.readLine();
        System.out.print("\nEnter the destination aws secret: ");
        String awsSecret = bufferRead.readLine();
        System.out.println("\nStarting...");
        jobExecutor.startMigration(sendId, awsKey, awsSecret, bucket);

    }
}
