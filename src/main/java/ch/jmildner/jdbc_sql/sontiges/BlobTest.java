package ch.jmildner.jdbc_sql.sontiges;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BlobTest
{

    private static final String DRV = "org.h2.Driver";
    private static final String URL = "jdbc:h2:tcp://localhost:9092/~/test";
    private static final String USR = "sa";
    private static final String PWD = "sa";

    public static void main(String[] args) throws Exception
    {
        Class.forName(DRV);
        Connection c = DriverManager.getConnection(URL, USR, PWD);

        Statement s = c.createStatement();

        try
        {
            s.execute("drop table personMitBlob");
        }
        catch (SQLException e)
        {
        }

        s.execute("create table personMitBlob (id int primary key,"
                + " name varchar(20), blobtype char(3), blobobject longblob)");

        s.execute("insert into personMitBlob values(1001,'hugo',null,null)");
        s.execute("insert into personMitBlob values(1002,'max',null,null)");

        String sql = "insert into personMitBlob values(?,?,?,?)";
        PreparedStatement ps = c.prepareStatement(sql);

        ps.setInt(1, 1003);
        ps.setString(2, "unbekannt");
        ps.setString(3, "png");
        ps.setBlob(4, new FileInputStream("data/unbekannt.png"));
        ps.execute();

        ps.close();

        s.execute("select id,name,blobtype,blobobject from personMitBlob");

        ResultSet rs = s.getResultSet();

        while (rs.next())
        {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            String blobtype = rs.getString("blobtype");
            Blob blobobjet = rs.getBlob("blobobject");

            System.out.println(id + "\t" + name + "\t" + blobtype + "\t" + blobobjet);

            if ("png".equals(blobtype))
            {
                FileOutputStream fos = new FileOutputStream("data/bild-von-" + name + ".png");
                fos.write(blobobjet.getBytes(1, (int) blobobjet.length()));
                fos.close();
            }

        }  

        rs.close();

        c.close();

        System.out.println("programm beendet");
    }
}
