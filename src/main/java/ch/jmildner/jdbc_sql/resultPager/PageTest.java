package ch.jmildner.jdbc_sql.resultPager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.TestDatenTools;

public class PageTest
{

    private static Connection c;

    public static void main(String[] args) throws Exception
    {
        c = new MyDataSource("H2").getConnection();
        c.setAutoCommit(true);

        testdatenVorbereiten();

        testdatenAusgeben();

        testdatenSeitenweiseAusgeben();

        c.close();

        System.out.println("programm beendet");
    }

    private static void testdatenSeitenweiseAusgeben()
            throws SQLException
    {
        Statement s1 = c.createStatement();
        Statement s2 = c.createStatement();

        s1.execute("select id,name,wert from testdaten");
        ResultSet rsDaten = s1.getResultSet();

        s2.execute("select count(*) from testdaten");
        ResultSet rsCount = s2.getResultSet();

        ResultPager pager = new ResultPager(10, rsCount);

        System.out.println("PAGES: " + pager.getPages());

        for (int page = 0; page < pager.getPages(); page++)
        {
            System.out.println("SEITE " + (page + 1));
            List<?> results = pager.getResultsOfPage(rsDaten);
            results.forEach((o) ->
            {
                System.out.println(o);
            });
            pager.next();
        }

        pager.back(); // eine Page zurückschalten
    }

    private static void testdatenAusgeben() throws Exception
    {
        Statement s = c.createStatement();

        s.execute("select id,name,wert from testdaten");

        ResultSet rs = s.getResultSet();

        while (rs.next())
        {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int wert = rs.getInt("wert");
            String zeile = String.format("%5d %10d %s %n", id, wert,
                    name);
            System.out.printf(zeile);
        }

        rs.close();

        s.close();
    }

    private static void testdatenVorbereiten() throws Exception
    {
        Statement s = c.createStatement();

        try
        {
            s.execute("drop table testdaten");
        }
        catch (Exception e)
        {
        }

        s.execute(
                "create table testdaten (id int primary key, name varchar(100),  wert int)");

        String sql = "insert into testdaten values(?,?,?)";
        PreparedStatement ps = c.prepareStatement(sql);

        for (int i = 1; i <= 105; i++)
        {
            ps.setInt(1, i);
            ps.setString(2, TestDatenTools.getName());
            ps.setInt(3, MyTools.getRandom(1000000000, 2000000000));
            ps.execute();
        }
        ps.close();

        s.close();
    }
}
