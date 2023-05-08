import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 */
public class DownloadFile {
    static boolean Download(String fileUrl, String dir){
        System.out.println("Downing " + fileUrl + " start");
        String[] split = fileUrl.split("/");
        System.out.println("filename is: " + split[split.length - 1]);
        String fileName = dir + System.getProperty("file.separator") + split[split.length - 1];

        if(new File(fileName).exists()){
            System.out.println("文件已经存在，不再重复下载");
            return true;
        }

        try {
            URL url = new URL(fileUrl);
            URLConnection conn = url.openConnection();
            InputStream inputStream = conn.getInputStream();
            FileOutputStream outputStream = new FileOutputStream(fileName);

            byte[] buffer = new byte[2048];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, len);
            }

            outputStream.close();
            inputStream.close();
        }
        catch (Exception e){
            e.printStackTrace();
            System.out.println("Downing " + fileUrl + " FAILED");
            return false;
        }
        System.out.println("Downing " + fileUrl + " OK");
        return true;
    }

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        for (int i = 1; i < 100_000; i++){
            int finalI = i;
            int finalI1 = i;
            executorService.execute(() -> {
                String url = "https://www.gutenberg.org/cache/epub/" + finalI + "/pg" + finalI1 + ".txt.utf8";
                Download(url, "Y:\\txt\\");
            });

        }
        // 关闭线程池
        executorService.shutdown();
    }
}
