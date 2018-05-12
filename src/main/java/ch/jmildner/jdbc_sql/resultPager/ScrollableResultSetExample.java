package ch.jmildner.jdbc_sql.resultPager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.TestDatenTools;

public class ScrollableResultSetExample
{

    Connection connection;

    public static void main(String[] args) throws Exception
    {
        MyTools.uebOut("ScrollableResultSetExample", 2);

        showMemory("start");

        ScrollableResultSetExample scrollableResultSetExample = new ScrollableResultSetExample();
        scrollableResultSetExample.doit();
     //   scrollableResultSetExample = null;

        MyTools.uebOut("Programmende mit gc()", 2);
        System.gc();
        showMemory("nach gc");
    }

    public ScrollableResultSetExample() throws Exception
    {
        MyTools.uebOut("Constructor", 2);
        showMemory("vor  new MyDataSource");
        connection = new MyDataSource("H2").getConnection();
        showMemory("nach new MyDataSource");
        connection.setAutoCommit(true);
    }

    void doit() throws Exception
    {
        testdatenVorbereiten(25_000);

        navigierenImResultSet();

        testdatenAusgeben();
    }

    private void testdatenAusgeben() throws SQLException
    {
        MyTools.uebOut("Testdaten Ausgeben", 2);

        showMemory("vor ausgeben");

        Statement s = connection.createStatement();

        showMemory("vor s.execute");
        s.execute("select id,name,wert from testdaten2");

        showMemory("nach s.execute");

        ResultSet rs = s.getResultSet();

        showMemory("nach getResultSet()");

        while (rs.next())
        {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int wert = rs.getInt("wert");
            String zeile = String.format("%5d %10d %s %n", id, wert,
                    name);
            if (id % 10000 == 0)
            {
                System.out.printf(zeile);
            }
        }

        showMemory("nach while");

        rs.close();

        showMemory("nach rs.close()");

        s.close();

        showMemory("nach s.close()");

    }

    private void navigierenImResultSet() throws SQLException
    {
        Statement statement = createScrollableInsensitiveStatement();

        MyTools.uebOut("Navigieren im ResultSet", 2);

        showMemory("vor results=executeQuery(SELECT ..)");

        ResultSet results = statement
                .executeQuery("SELECT * FROM testdaten2");

        showMemory("nach results=executeQuery(SELECT ..)");
        showMemory("vor Navigieren im ResultSet");

        System.out.println("begin      \t" + results.getRow()
                + ", is before first ? " + results.isBeforeFirst());

        results.next();

        System.out.println("next      \t" + results.getRow()
                + ", is first ? " + results.isFirst());

        results.next();

        System.out.println("next      \t" + results.getRow()
                + ", is first ? " + results.isFirst());

        results.last();

        System.out.println("last      \t" + results.getRow()
                + ", is last ? " + results.isLast());

        results.next();

        System.out.println("next      \t" + results.getRow()
                + ", is before first ? " + results.isBeforeFirst()
                + ", is after last ? " + results.isAfterLast());

        results.afterLast();

        System.out.println("after last\t" + results.getRow()
                + ", is after last ? " + results.isAfterLast());

        results.next();

        System.out.println("next      \t" + results.getRow()
                + ", is first ? " + results.isFirst());

        results.absolute(3);

        System.out.println("absolute 3\t" + results.getRow());

        results.absolute(1);

        System.out.println("absolute 1\t" + results.getRow()
                + ", is first ? " + results.isFirst());

        results.absolute(-1);

        System.out.println("absolute -1 \t" + results.getRow()
                + ", is last ? " + results.isLast());

        results.absolute(-4);

        System.out.println("absolute -4\t" + results.getRow());

        results.relative(5);

        System.out.println("relative 5\t" + results.getRow()
                + ", is after last ? " + results.isAfterLast());

        results.relative(-13);

        System.out.println("relative -13\t" + results.getRow()
                + ", is before first ? " + results.isBeforeFirst());

        results.previous();

        System.out.println("previous \t" + results.getRow());

        results.beforeFirst();

        System.out.println("before first\t" + results.getRow()
                + ", is before first ? " + results.isBeforeFirst());

        showMemory("nach Navigieren im ResultSet");

    }

    void testdatenVorbereiten(final int MAX) throws Exception
    {
        MyTools.uebOut(MAX + " Testdaten Vorbereiten", 2);

        showMemory("vor testdatenVorbereiten()");

        Statement s = connection.createStatement();

        try
        {
            s.execute("drop table testdaten2");
        }
        catch (Exception e)
        {
        }

        s.execute("create table testdaten2 (id int primary key, name varchar(100),  wert int)");

        String sql = "insert into testdaten2 values(?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);

        for (int i = 1; i <= MAX; i++)
        {
            ps.setInt(1, i);
            ps.setString(2, TestDatenTools.getName());
            ps.setInt(3, MyTools.getRandom(1000000000, 2000000000));
            ps.execute();
        }
        ps.close();

        s.close();

        showMemory("nach testdatenVorbereiten()");
    }

    private Statement createScrollableInsensitiveStatement()
            throws SQLException
    {
        MyTools.uebOut("createScrollableInsensitiveStatement", 2);

        showMemory("vor createStatement()");
        /*
		 * An insensitive scrollable result set is one where the values
		 * captured in the result set never change, even if changes are
		 * made to the table from which the data was retrieved.
		 * 
		 * A sensitive scrollable result set is one where the current
		 * values in the table are reflected in the result set.
		 * 
		 * So if a change is made to a row in the table, the result set
		 * will show the new data when the cursor is moved to that row
         */

        // Create an insensitive scrollable result set
        //
        // for sensitive scrollable result sets use
        // ResultSet.TYPE_SCROLL_SENSITIVE directive
        Statement s = connection.createStatement( //
                ResultSet.TYPE_SCROLL_INSENSITIVE, //
                ResultSet.CONCUR_READ_ONLY //
        );

        showMemory("nach createStatement()");

        return s;
    }

    static private void showMemory(String m)
    {
        System.out.printf(
                "%40s free:     %,13d     total:%,13d    belegt:%,13d     max:%,13d %n",
                m //
                ,
                 Runtime.getRuntime().freeMemory() //
                ,
                 Runtime.getRuntime().totalMemory() //
                ,
                 Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory() //
                ,
                 Runtime.getRuntime().maxMemory() //
        );
    }

}
