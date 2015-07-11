package me.yonatan.bitscape.model;

import lombok.Data;

/**
 * Created by yonatan on 10/7/2015.
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
