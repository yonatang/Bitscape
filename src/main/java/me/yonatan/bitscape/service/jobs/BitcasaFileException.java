package me.yonatan.bitscape.service.jobs;

import lombok.Getter;
import me.yonatan.bitscape.model.BitcasaFile;

/**
 * Created by yonatan on 11/7/2015.
 */
public class BitcasaFileException extends Exception {
    @Getter
    private BitcasaFile bitcasaFile;

    public BitcasaFileException(BitcasaFile file, String message, Exception e) {
        super(message, e);
        this.bitcasaFile = file;
    }

    public BitcasaFileException(BitcasaFile file, String message) {
        super(message);
        this.bitcasaFile = file;
    }
}
