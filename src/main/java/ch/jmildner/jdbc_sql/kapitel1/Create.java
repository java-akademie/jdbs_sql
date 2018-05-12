package ch.jmildner.jdbc_sql.kapitel1;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;
import java.sql.SQLException;

public class Create
{

    public static void main(String[] args) 
    {
        doit("ORACLE");
        doit("POSTGRES");
        doit("H2");
        doit("MYSQL");
    }

    static void doit(String dbName)
    {
        MyTools.uebOut("Start Create doit() with DB " + dbName, 3);

        try (Connection c = new MyDataSource(dbName).getConnection())
        {
            c.setAutoCommit(true);

            drop_create(c);
        }
        catch (Exception e)
        {
            System.out.println("------------ EXCEPTION ------------");
            System.out.println(e.getMessage());
            System.out.println("------------ EXCEPTION ------------");
            
            MyTools.pause();
        }

        MyTools.untOut("Stopp Create doit() with DB " + dbName, 3);
    }

    private static void drop_create(Connection c) throws SQLException
    {

        try (Statement s = c.createStatement())
        {
            MyTools.uebOut("start drop, create und insert table/view", 2);

            SqlTools.execute(s, "drop view vpers11",false);
            SqlTools.execute(s, "drop view vpers12",false);
            SqlTools.execute(s, "drop table pers1",false);

            SqlTools.execute(s, "create table pers1 (id int primary key, name varchar(40), kz int)");
            SqlTools.execute(s, "create view vpers11 as select id, name, kz from pers1 order by name");
            SqlTools.execute(s, "create view vpers12 as select id, name, kz from pers1 order by kz");

            SqlTools.executeUpdate(s, "insert into pers1 values(1, 'huber'  , 8 )");
            SqlTools.executeUpdate(s, "insert into pers1 values(2, 'lenz '  , 7 )");
            SqlTools.executeUpdate(s, "insert into pers1 values(3, 'meier'  , null )");
            SqlTools.executeUpdate(s, "insert into pers1 values(4, 'gruber' , 3 )");
            SqlTools.executeUpdate(s, "insert into pers1 values(5, 'banz'   , 9 )");
            SqlTools.executeUpdate(s, "insert into pers1 values(6, 'benz'   , 1 )");
            MyTools.untOut("stopp drop, create und insert table/view", 2);

            MyTools.uebOut("start select table/view", 2);
            SqlTools.select(s, "select * from pers1");
            SqlTools.select(s, "select * from vpers11");
            SqlTools.select(s, "select * from vpers12");
            MyTools.untOut("stopp select table/view", 2);

            MyTools.uebOut("start update und select", 2);
            SqlTools.executeUpdate(s, "update pers1 set name='hugo'");
            SqlTools.select(s, "select * from pers1");
            MyTools.untOut("stopp update und select", 2);

            MyTools.uebOut("start delete und select", 2);
            SqlTools.executeUpdate(s, "delete from pers1");
            SqlTools.select(s, "select * from pers1");
            MyTools.untOut("stopp delete und select", 2);
        }

    }

}
