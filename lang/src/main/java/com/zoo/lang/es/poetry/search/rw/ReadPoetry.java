package com.zoo.lang.es.poetry.search.rw;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoo.lang.es.poetry.search.Poetry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Author: JMD
 * @Date: 3/14/2023
 */
public class ReadPoetry {
    public static List<Poetry> read(String filePath) throws ClassNotFoundException, IOException {

        ObjectMapper mapper = new ObjectMapper();

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Class.forName("oracle.jdbc.driver.OracleDriver");

        return mapper.readValue(new File(filePath), new TypeReference<List<Poetry>>() {});
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ArrayList<String> files = new ArrayList<String>();
        // files.add("Y:\\github\\chinese-poetry\\chuci\\chuci.json");

        //
        File ciPath = new File("Y:\\github\\chinese-poetry\\ci\\");
        for(File file: Objects.requireNonNull(ciPath.listFiles())){
            System.out.println(file.getName());
            if(file.getName().startsWith("ci") || file.getName().startsWith("宋词")){
                if(file.getName().endsWith(".json")){
                    files.add(file.getAbsolutePath());
                }
            }
        }
        System.out.println("size: " + files.size());

        for (String filepath: files){
            List<Poetry> ret = read(filepath);
            System.out.println(filepath);
            System.out.println(ret.get(0));
        }
    }
}
