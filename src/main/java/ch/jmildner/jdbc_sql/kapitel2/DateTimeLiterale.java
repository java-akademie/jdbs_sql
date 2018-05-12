package ch.jmildner.jdbc_sql.kapitel2;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;
import java.sql.SQLException;

public class DateTimeLiterale
{

    public static void main(String[] args)
    {
        doit("ORACLE");
        doit("POSTGRES");
        doit("MYSQL");
        doit("H2");
    }

    static private void doit(String dbName)
    {
        MyTools.uebOut("Start DateTimeLiterale doit() with DB " + dbName, 3);

        try (Connection c = new MyDataSource(dbName).getConnection())
        {
            dateTimeLiterale(dbName, c);
        }
        catch (Exception e)
        {
            System.out.println("------------ EXCEPTION ------------");
            System.out.println(e.getMessage());
            System.out.println("------------ EXCEPTION ------------");

            MyTools.pause();
        }

        MyTools.untOut("Stopp DateTimeLiterale doit() with DB " + dbName, 3);
    }

    static private void dateTimeLiterale(String dbName, Connection c) throws SQLException
    {
        try (Statement s = c.createStatement())
        {
            switch (dbName)
            {
                case ("ORACLE"):
                    dateTimeLiterale_ORACLE(s);
                    break;
                case ("POSTGRES"):
                    dateTimeLiterale_POSTGRES(s);
                    break;
                case ("MYSQL"):
                    dateTimeLiterale_MYSQL(s);
                    break;
                case ("H2"):
                    dateTimeLiterale_H2(s);
                    break;
                default:
                    break;
            }

            dateTimeLiterale_ALLE(s, dbName);
        }
    }

    static private void dateTimeLiterale_ORACLE(Statement s)
    {
        SqlTools.executeUpdate(s, "drop table zeit22", false);
        SqlTools.executeUpdate(s, "create table zeit22 (id int primary key, datum date, zeitstempel timestamp)");
        SqlTools.executeUpdate(s, "insert into zeit22 values(4, to_date('24-10-1947','dd-mm-yyyy'), to_timestamp('2015/02/28 17:30:50', 'yyyy/mm/dd hh24:mi:ss'))");
        SqlTools.executeUpdate(s, "insert into zeit22 values(5, '29/02/2000', '28/02/2001 09:30:50')");
        SqlTools.executeUpdate(s, "insert into zeit22 values(6, '01/02/2000', '29/02/2000 10:30:50')");
        SqlTools.executeUpdate(s, "insert into zeit22 values(9, current_date, current_timestamp)");

        SqlTools.select(s, "select * from zeit22", 3);
        SqlTools.select(s, "select to_char(datum,'yyyy/mm/dd') from zeit22", 3);
        SqlTools.select(s, "select to_char(zeitstempel,'yyyy/mm/dd hh24:mi:ss') from zeit22", 3);
        SqlTools.select(s, "select to_char(zeitstempel,'dd') as zeitstempel from zeit22", 3);
        SqlTools.select(s, "select to_char(datum,'dd.mm.yyyy') as datum from zeit22", 3);
        SqlTools.select(s, "select datum,datum+1  from zeit22", 3);
    }

    static private void dateTimeLiterale_POSTGRES(Statement s)
    {
        SqlTools.executeUpdate(s, "drop table zeit23", false);

        SqlTools.executeUpdate(s, "create table zeit23 (id int primary key, datum date, zeitstempel timestamp, zeit time)");

        SqlTools.executeUpdate(s, "insert into zeit23 values(1 , '2015.07.13' ,'2015/07/14 21:30:44' ,'17:33:00')");
        SqlTools.executeUpdate(s, "insert into zeit23 values(2 , '2015-07-14' ,'2015/07/14 21:30:45' ,null)");
        SqlTools.executeUpdate(s, "insert into zeit23 values(3 , '1947/10/24' ,'2015/07/22 21:30:46' ,null) ");
        SqlTools.executeUpdate(s, "insert into zeit23 values(4 , to_date('24-10-1947', 'dd-mm-yyyy'),to_timestamp('2015/02/28 17:30:50','yyyy/mm/dd hh24:mi:ss') ,null)");
        SqlTools.executeUpdate(s, "insert into zeit23 values(6 , '2000/02/13' ,'2000/02/29 17:30:50' ,null)");
        SqlTools.executeUpdate(s, "insert into zeit23 values(7 , '2015-10-24' ,'2015-02-28 17:30:50' ,'17:25:00')");
        SqlTools.executeUpdate(s, "insert into zeit23 values(8 , current_date ,current_timestamp ,current_time)");

        SqlTools.select(s, "select * from zeit23");
        SqlTools.select(s, "select to_char(datum,'yyyy/mm/dd') from zeit23");
        SqlTools.select(s, "select to_char(zeitstempel,'yyyy/mm/dd hh24:mi:ss') from zeit23");
        SqlTools.select(s, "select to_char(zeitstempel,'dd') as zeitstempel from zeit23");
        SqlTools.select(s, "select to_char(datum,'dd.mm.yyyy') as datum from zeit23");
        SqlTools.select(s, "select datum, datum+1 from zeit23");
    }

    static private void dateTimeLiterale_MYSQL(Statement s)
    {
        SqlTools.executeUpdate(s, "drop table zeit24", false);
        
        SqlTools.executeUpdate(s, "create table zeit24 (id int primary key, datum date, zeitstempel1 timestamp, zeitstempel2 timestamp, zeit time)");
        
        SqlTools.executeUpdate(s, "insert into zeit24 values(1,'2015.07.13',  null,'2015/07/14 21:30:44', null)");
        SqlTools.executeUpdate(s, "insert into zeit24 values(2 ,'2015-07-14', null,'2015/07/14 21:30:45',null)");
        SqlTools.executeUpdate(s, "insert into zeit24 values(3,'1947/10/24',  null,'2015/07/22 21:30:46',null)");
        SqlTools.executeUpdate(s, "insert into zeit24 values(7,'20151024',    null,'2015-02-28 17:30:50','15:25:15')");
        SqlTools.executeUpdate(s, "insert into zeit24 values(8,current_date,  null,current_timestamp,current_time)");
        
        SqlTools.select(s, "select * from zeit24");
        SqlTools.select(s, "select datum, datum + 1  from zeit24");
    }

    static private void dateTimeLiterale_H2(Statement s)
    {
        SqlTools.executeUpdate(s, "drop table zeit21", false);
        
        SqlTools.executeUpdate(s, "create table zeit21 (id int primary key,datum date, zeitstempel timestamp, zeit time)");
        
        SqlTools.executeUpdate(s, "insert into zeit21 values(7, '2015-10-24', '2015-02-28 17:30:50', '12:15:30')");
        SqlTools.executeUpdate(s, "insert into zeit21 values(8, current date, current timestamp, current time)");
        
        SqlTools.select(s, "select * from zeit21");
        SqlTools.select(s, "select to_char(datum,'yyyy/mm/dd') from zeit21");
        SqlTools.select(s, "select to_char(zeitstempel,'yyyy/mm/dd hh24:mi:ss') as zeitstempel from zeit21");
        SqlTools.select(s, "select to_char(zeitstempel,'dd') as zeitstempel_dd from zeit21");
        SqlTools.select(s, "select to_char(datum,'dd.mm.yyyy') as datum from zeit21");
        SqlTools.select(s, "select datum, (datum + 1) as datum_plus_1 from zeit21");
    }

    static private void dateTimeLiterale_ALLE(Statement s, String dbName)
    {
        String currentDate = "";
        String currentTime = "";
        String currentTimestamp = "";

        switch (dbName)
        {
            case "H2":
            case "DB2":
                currentDate = "current date";
                currentTime = "current time";
                currentTimestamp = "current timestamp";
                break;
            case "MYSQL":
            case "POSTGRES":
            case "ORACLE":
                currentDate = "current_date";
                currentTime = "current_time";
                currentTimestamp = "current_timestamp";
                break;
            case "SQLSERVER":
                // es gibt nur sysdatetime
                break;
            default:
                break;
        }

        SqlTools.executeUpdate(s, "drop table zeit27");

        switch (dbName)
        {
            case "SQLSERVER":
                SqlTools.executeUpdate(s, "create table zeit27 (id int ,datum date, zeit time, zeitstempel datetime)");
                SqlTools.executeUpdate(s, "insert into zeit27(id,datum,zeit,zeitstempel) values(10,'2000-02-22','09:30:00','2000-02-22 09:30:00')");
                SqlTools.executeUpdate(s, "insert into zeit27 values(11,sysdatetime(), sysdatetime(), sysdatetime())");
                SqlTools.select(s, "select * from zeit27");
                break;

            case "ORACLE":
                SqlTools.executeUpdate(s, "create table zeit27 (id int , datum date, zeitstempel timestamp)");
                SqlTools.executeUpdate(s, "insert into zeit27 values(10, '29/02/2000', '28/02/2001 09:30:50')");
                SqlTools.executeUpdate(s, "insert into zeit27 values(11, " + currentDate + "," + currentTimestamp + ")");
                SqlTools.select(s, "select * from zeit27");
                break;

            default:
                SqlTools.executeUpdate(s, "create table zeit27 (id int,datum date, zeitstempel timestamp, zeit time)");
                SqlTools.executeUpdate(s, "insert into zeit27 values(10,'2015-10-24', '2015-02-28 17:30:50', '12:15:30')");
                SqlTools.executeUpdate(s, "insert into zeit27 values(11, " + currentDate + "," + currentTimestamp + "," + currentTime + ")");
                SqlTools.select(s, "select * from zeit27");
                break;
        }
    }

}
