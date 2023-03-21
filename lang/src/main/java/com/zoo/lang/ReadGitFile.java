package com.zoo.lang;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

/**
 * @Author: JMD
 * @Date: 3/16/2023
 */
public class ReadGitFile {
    static String ReadFile(String fileUrl){
        URL url;
        StringBuilder file= new StringBuilder();

        try {
            url = new URL(fileUrl);
            URLConnection uc;
            uc = url.openConnection();

            uc.setRequestProperty("X-Requested-With","Curl");
            ArrayList<String> list=new ArrayList<String>();

            BufferedReader reader = new BufferedReader(new InputStreamReader(uc.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null){
                file.append(line);
            }

            System.out.println(file);
            return file.toString();

        } catch (IOException e) {
            System.out.println("Wrong username and password");
            return null;
        }
    }
    public static void main(String[] args) {

        System.out.println(ReadFile("https://github.com/chinese-poetry/chinese-poetry/blob/master/caocaoshiji/caocao.json"));
    }
}


