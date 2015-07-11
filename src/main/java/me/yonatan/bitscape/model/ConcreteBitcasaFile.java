package me.yonatan.bitscape.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.beans.BeanUtils;

import java.io.File;

/**
 * Created by yonatan on 11/7/2015.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ConcreteBitcasaFile extends BitcasaFile {
    public ConcreteBitcasaFile(BitcasaFile bitcasaFile, File file) {
        BeanUtils.copyProperties(bitcasaFile, this);
        this.downloadedFile = file;
    }

    private File downloadedFile;
}
