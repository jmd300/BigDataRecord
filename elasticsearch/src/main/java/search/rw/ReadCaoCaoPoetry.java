package search.rw;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import search.OracleConf;
import search.Poetry;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author: JMD
 * @Date: 3/14/2023
 */
public class ReadCaoCaoPoetry {
    static String filePath = "Y:\\github\\chinese-poetry\\caocaoshiji\\caocao.json";


    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        ObjectMapper mapper = new ObjectMapper();

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Class.forName("oracle.jdbc.driver.OracleDriver");
        List<Poetry> poetries = mapper.readValue(new File(filePath), new TypeReference<List<Poetry>>() {});

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = OracleConf.getConnection();
            ps = conn.prepareStatement("INSERT INTO POETRY VALUES(?, ?, ?, ?, ?)");
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            for(Poetry poetry: poetries){
                poetry.setAuthor("曹操");
                System.out.println(poetry);
                ps.setString(1, poetry.getTitle());
                ps.setString(2, poetry.getAuthor());
                ps.setString(3, poetry.getVerse());
                ps.setString(4, String.valueOf(poetry.hashCode()));
                ps.setString(5, poetry.getNote());

                boolean execute = ps.execute();
                System.out.println(execute);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            conn.close();
            if(ps != null){
                ps.close();
            }
        }

    }
}
