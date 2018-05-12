package ch.jmildner.jdbc_sql.sontiges;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;

public class ZeitMessung
{

    static String sqlDrop = "drop table a99";
    static String sqlCreate = "create table a99 (id int not null primary key, wert int) ";

    public static void main(String[] args) throws Exception
    {
        MyTools.uebOut("ZeitMessung", 2);

        Connection c = new MyDataSource().getConnection();

        testGetConnection(1_000);

        testCreateStatement(c, 1_000_000);

        MyTools.uebOut("Programmende", 2);

    }

    private static void testGetConnection(final int MAX)
            throws SQLException, ClassNotFoundException
    {
        final String DRV = "org.h2.Driver";
        final String URL = "jdbc:h2:tcp://localhost:9092/~/test";
        final String USR = "sa";
        final String PWD = "sa";

        long start = System.currentTimeMillis();

        Class.forName(DRV);

        for (int i = 1; i <= MAX; i++)
        {
            Connection c = DriverManager.getConnection(URL, USR, PWD);
            c.close();
        }

        long stopp = System.currentTimeMillis();

        System.out.printf(
                "Dauer %,12d getConnections  %,5d Millisecunden%n", MAX,
                (stopp - start));
    }

    private static void testCreateStatement(Connection c, final int MAX)
            throws SQLException
    {
        long start = System.currentTimeMillis();

        for (int i = 1; i <= MAX; i++)
        {
            Statement s = c.createStatement();
            s.close();
        }

        long stopp = System.currentTimeMillis();

        System.out.printf(
                "Dauer %,12d createStatement %,5d Millisecunden%n", MAX,
                (stopp - start));
    }

}
