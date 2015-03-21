package me.yonatan.model;

import lombok.Data;

/**
 * Created by yonatan on 12/1/14.
 */
@Data
public class BitcasaFile {
    private String name;
    private long size;
    private String type; //file, folder
    private String id;
    private String payload;
    private String digest;
    private String nonce;
    private String path;

}
