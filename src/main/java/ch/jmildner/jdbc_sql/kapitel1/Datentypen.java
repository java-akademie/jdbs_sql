package ch.jmildner.jdbc_sql.kapitel1;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;
import java.sql.SQLException;

public class Datentypen
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
        MyTools.uebOut("Start Datentypen doit() with DB " + dbName, 3);

        try (Connection c = new MyDataSource(dbName).getConnection())
        {
            c.setAutoCommit(true);
            datentypen(dbName, c);
        }
        catch (Exception e)
        {
            System.out.println("------------ EXCEPTION ------------");
            System.out.println(e.getMessage());
            System.out.println("------------ EXCEPTION ------------");

            MyTools.pause();
        }

        MyTools.untOut("Stopp Datentypen doit() with DB " + dbName, 3);
    }

    static void datentypen(String dbName, Connection c) throws SQLException
    {
        try (Statement s = c.createStatement())
        {
            MyTools.uebOut("DATENTYPEN", 2);

            datentypen1(s, dbName);
            datentypen2(s);
            datentypen3(s, dbName);
        }
    }

    static void datentypen1(Statement s, String dbName)
    {
        MyTools.uebOut(
                "char/varchar/int/bigint (ORACE kennt kein BIGINT)", 2);

        if (dbName.equals("ORACLE"))
        {
            SqlTools.executeUpdate(s, "drop table mitarb111", false);
            SqlTools.executeUpdate(s, "create table mitarb111 (id int primary key, name varchar(20), sex char(1))");
            SqlTools.executeUpdate(s, "insert into mitarb111 values(1, 'meier', 'm')");
            SqlTools.executeUpdate(s, "insert into mitarb111 values(2, 'huber', 'w')");
            SqlTools.executeUpdate(s, "insert into mitarb111 values(3, 'luger', 'm')");
            SqlTools.select(s, "select * from mitarb111", 1);
        }
        else
        {
            SqlTools.executeUpdate(s, "drop table mitarb112");
            SqlTools.executeUpdate(s, "create table mitarb112 (id int primary key, name varchar(20), sex char(1), zahl bigint)");
            SqlTools.executeUpdate(s, "insert into mitarb112 values(1, 'meier', 'm', 100000002)");
            SqlTools.executeUpdate(s, "insert into mitarb112 values(2, 'huber', 'w', 100000003)");
            SqlTools.executeUpdate(s, "insert into mitarb112 values(3, 'luger', 'm', 100000004)");
            SqlTools.select(s, "select * from mitarb112", 3);
        }
    }

    static void datentypen2(Statement s)
    {
        MyTools.uebOut("decimal", 2);

        /**
         * betrag
         *
         * <code>
         * 		decimal(31,5)
         * 			bedeutet 31 Stellen insgesamt
         * 			   davon  5 Dezimalstellen
         *
         * 		DB2 		31
         * 		Oracle 		38
         *		MySql 		65
         * 		Postgres        1000
         * 		H2		 >4000
         * </code>
         *
         */
        SqlTools.executeUpdate(s, "drop table mitarb12", false);
        SqlTools.executeUpdate(s, "create table mitarb12 (id int ,name varchar(20),betrag decimal(31,2),saldo decimal(9,2))");
        SqlTools.executeUpdate(s, "insert into mitarb12 values(1, 'meier', 123456.78901523, 112.5)");
        SqlTools.executeUpdate(s, "insert into mitarb12 values(2, 'huber', 12345.33, 112.5)");
        SqlTools.executeUpdate(s, "insert into mitarb12 values(3, 'luger1', 12345.44, 112.5)");
        SqlTools.executeUpdate(s, "insert into mitarb12 values(4, 'luger2', 99999999999999999.99, 22112.5)");
        SqlTools.executeUpdate(s, "insert into mitarb12 values(5, 'luger3', -99999999999999999.99, 11312.5)");
        SqlTools.executeUpdate(s, "insert into mitarb12 values(6, 'luger4', 99999999999999999.95999, 1512.5)");
        SqlTools.executeUpdate(s, "insert into mitarb12 values(7, 'luger5', 12345.445565, 112.5)");

        SqlTools.select(s, "select * from mitarb12", 3);
    }

    static void datentypen3(Statement s, String dbName)
    {
        MyTools.uebOut("date, time, timestamp (ORACLE kennt kein TIME)",
                2);

        if (dbName.equals("ORACLE"))
        {
            SqlTools.executeUpdate(s, "drop table zeit131", false);
            SqlTools.executeUpdate(s, "create table zeit131 (id int primary key, datum date, zeitstempel timestamp)");
            SqlTools.executeUpdate(s, "insert into zeit131 values(1, current_date, current_timestamp)");
            SqlTools.select(s, "select * from zeit131", 3);
        }
        else
        {
            SqlTools.executeUpdate(s, "drop table zeit132", false);
            SqlTools.executeUpdate(s, "create table zeit132 (id int primary key, datum date, zeitstempel timestamp, zeit time)");
            SqlTools.executeUpdate(s, "insert into zeit132 values(1, current_date, current_timestamp, current_time)");
            SqlTools.select(s, "select * from zeit132", 3);
        }
    }
}
