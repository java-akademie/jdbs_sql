package ch.jmildner.jdbc_sql.kapitel5;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;

public class U5_ReferenzielleIntegritaet
{

    private static final String OS = "MAC";

    public static void main(String[] args) throws Exception
    {
        // doit("DB2");
        // doit("ORACLE_EUROPA");
        // doit("SQLSERVER");
        // doit("POSTGRES");
        // doit("MYSQL");
        doit("H2");

        MyTools.uebOut("Programmende", 2);
    }

    static void doit(String dbName)
    {
        MyTools.sleep(500);
        MyTools.uebOut(dbName, 3);
        System.out.println("\n\n\n");
        MyTools.sleep(2500);

        if (OS.equals("MAC") && dbName.equals("DB2"))
        {
            System.out.println("DB2 funktioniert nur unter Windows");
            return;
        }

        try
        {
            Connection c = new MyDataSource(dbName).getConnection();
            Statement s = c.createStatement();

            a05(dbName, s);
        }
        catch (Exception e)
        {
        }
    }

    static void a05(String dbName, Statement s)
    {
        a051(s);
        a052(s, dbName);
        a053(s, dbName);
    }

    static void a051(Statement s)
    {
        MyTools.uebOut("***** a051 - foreign key *****", 2);

        SqlTools.executeUpdate(s, "drop table pers51");
        SqlTools.executeUpdate(s, "drop table addr51");

        SqlTools.executeUpdate(s, "create table addr51(id int not null primary key" + ",ort varchar(20))");

        SqlTools.executeUpdate(s, "create table pers51(id int not null primary key" + ",name varchar(20) "
                + ",addrId int REFERENCES addr51(id))");

        SqlTools.executeUpdate(s, "insert into addr51 values(1, 'basel')");
        SqlTools.executeUpdate(s, "insert into addr51 values(2, 'bern' )");

        SqlTools.executeUpdate(s, "insert into pers51 values(1, 'huber',  1)");
        SqlTools.executeUpdate(s, "insert into pers51 values(2, 'gerber', 1)");
        SqlTools.executeUpdate(s, "insert into pers51 values(3, 'meier',  2)");

        SqlTools.select(s, "select * from pers51");
        SqlTools.select(s, "select * from addr51");
    }

    static void a052(Statement s, String dbName)
    {
        MyTools.uebOut("***** a052 - foreign key *****", 2);

        if (dbName.equals("MYSQL"))
        {
            SqlTools.executeUpdate(s, "alter table pers52 drop foreign key fk_addr52");
        }
        else
        {
            SqlTools.executeUpdate(s, "alter table pers52 drop constraint fk_addr52");
        }

        SqlTools.executeUpdate(s, "drop table addr52");
        SqlTools.executeUpdate(s, "drop table pers52");

        SqlTools.executeUpdate(s, "create table addr52(id int not null primary key" + ",ort varchar(20))");

        SqlTools.executeUpdate(s,
                "create table pers52(id int not null primary key" + ",name varchar(20)        "
                + ",addrId int              " + ",CONSTRAINT fk_addr52      "
                + "    FOREIGN KEY (addrId) REFERENCES addr52(id))");

        SqlTools.executeUpdate(s, "insert into addr52 values(1, 'basel')");
        SqlTools.executeUpdate(s, "insert into addr52 values(2, 'bern' )");

        SqlTools.executeUpdate(s, "insert into pers52 values(1, 'huber',  1)");
        SqlTools.executeUpdate(s, "insert into pers52 values(2, 'gerber', 1)");
        SqlTools.executeUpdate(s, "insert into pers52 values(5, 'berger', 2)");

        SqlTools.select(s, "select * from pers52");
        SqlTools.select(s, "select * from addr52");
    }

    static void a053(Statement s, String dbName)
    {
        MyTools.uebOut("***** a053 - foreign key *****", 2);

        if (dbName.equals("MYSQL"))
        {
            SqlTools.executeUpdate(s, "alter table pers53 drop foreign key fk_addr53");
        }
        else
        {
            SqlTools.executeUpdate(s, "alter table pers53 drop constraint fk_addr53");
        }

        SqlTools.executeUpdate(s, "drop table addr53");
        SqlTools.executeUpdate(s, "drop table pers53");

        SqlTools.executeUpdate(s, "create table pers53(id int not null primary key"
                + ",name varchar(20)        " + ",addrId int)");

        SqlTools.executeUpdate(s, "create table addr53(id int not null primary key" + ",ort varchar(20))");

        SqlTools.executeUpdate(s, "alter table pers53 add  constraint fk_addr53"
                + " foreign key(addrId) references addr53(id)");

        SqlTools.executeUpdate(s, "insert into addr53 values(1, 'basel')");
        SqlTools.executeUpdate(s, "insert into addr53 values(2, 'bern' )");

        SqlTools.executeUpdate(s, "insert into pers53 values(1, 'huber',  1)");
        SqlTools.executeUpdate(s, "insert into pers53 values(2, 'gerber', 1)");
        SqlTools.executeUpdate(s, "insert into pers53 values(5, 'berger', 2)");

        SqlTools.select(s, "select * from pers53");
        SqlTools.select(s, "select * from addr53");
    }

}
