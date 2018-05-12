package ch.jmildner.jdbc_sql.kapitel4;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class U4_Joins
{

    private static final String OS = "MAC";

    public static void main(String[] args)
    {
        doit("DB2");
        doit("ORACLE");
        doit("POSTGRES");
        doit("MYSQL");
        doit("H2");
    }

    static void doit(String dbName)
    {
        MyTools.uebOut("Start U4_Joins doit() with DB " + dbName, 3);

        MyTools.sleep(2000);

        if (OS.equals("MAC") && dbName.equals("DB2"))
        {
            System.out.println("DB2 funktioniert nur unter Windows");
            MyTools.pause();
        }
        else
        {
            try (Connection c = new MyDataSource(dbName).getConnection())
            {
                a04(dbName, c);
            }
            catch (Exception ex)
            {
                Logger.getLogger(U4_Joins.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        MyTools.untOut("Stopp U4_Joins doit() with DB " + dbName, 3);
    }

    static void a04(String dbName, Connection c) throws SQLException
    {
        try (Statement s = c.createStatement())
        {
            a041(s);
            a042(s, dbName);
            a043(s, dbName);
            a044(s);
        }
    }

    static void a041(Statement s)
    {
        MyTools.uebOut("***** a041 - (joins theta/equi) *****", 2);

        SqlTools.executeUpdate(s, "drop table addr41", false);
        SqlTools.executeUpdate(s, "drop table kund41", false);
        SqlTools.executeUpdate(s, "create table kund41  (id int not null primary key, name varchar(20), addrId int, kz1 int)");
        SqlTools.executeUpdate(s, "create table addr41 (id int not null primary key" + ",ort varchar(20), kz2 int)");

        SqlTools.executeUpdate(s, "insert into addr41 values(1, 'basel', 1)");
        SqlTools.executeUpdate(s, "insert into addr41 values(2, 'bern' , 1)");
        SqlTools.executeUpdate(s, "insert into addr41 values(3, 'paris', 1)");

        SqlTools.executeUpdate(s, "insert into kund41 values(1, 'huber'    , 1,1)");
        SqlTools.executeUpdate(s, "insert into kund41 values(2, 'gerber'   , 2,1)");
        SqlTools.executeUpdate(s, "insert into kund41 values(5, 'meier'    , 3,1)");

        // theta-join (immer noch kartesisches Produkt
        SqlTools.select(s, "select * from kund41, addr41 " + "where kund41.id > 1 and kund41.id < 3 order by 1");

        // equi-join (immer noch kartesisches Produkt)
        SqlTools.select(s, "select * from kund41, addr41 where kund41.id = 1 " + "order by 1");

        // joins (kein kartesisches Produkte mehr
        // w/where kund.addrId=addr.id)
        SqlTools.select(s, "select * from kund41, addr41 " + "where kund41.addrId = addr41.id order by 1");
    }

    static void a042(Statement s, String dbName)
    {
        MyTools.uebOut("***** a042 - (cross join, inner join[on,using]" + ", natural inner join) *****", 2);

        SqlTools.executeUpdate(s, "drop table kund42", false);
        SqlTools.executeUpdate(s, "drop table addr42", false);
        SqlTools.executeUpdate(s, "drop table ort42", false);
        SqlTools.executeUpdate(s, "create table ort42 (plz int not null primary key,ortsname varchar(20),  kz3 int)");
        SqlTools.executeUpdate(s, "create table addr42 (addrID int not null primary key,strasse varchar(20), plz int, kz2 int)");
        SqlTools.executeUpdate(s, "create table kund42 (id int not null primary key,name varchar(20), addrId int, kz1 int)");

        SqlTools.executeUpdate(s, "insert into ort42  values(1000, 'wien'        , 3)");
        SqlTools.executeUpdate(s, "insert into ort42  values(2100, 'korneuburg'  , 3)");

        SqlTools.executeUpdate(s, "insert into addr42 values(1, 'hauptstrasse 12',1000, 1)");
        SqlTools.executeUpdate(s, "insert into addr42 values(4, 'hauptplatz 3' 	 ,2100, 1)");

        SqlTools.executeUpdate(s, "insert into kund42 values(1, 'huber'    , 1,2)");
        SqlTools.executeUpdate(s, "insert into kund42 values(2, 'huber'    , 2,2)");
        SqlTools.executeUpdate(s, "insert into kund42 values(3, 'schuster' , 3,2)");

        // bis etwa 1992
        SqlTools.select(s, "select * from kund42 k, addr42 a, ort42 o where k.addrId = a.addrID and a.plz    = o.plz  order by 1");

        // cross join
        SqlTools.select(s, "select * from kund42 k " + "cross join addr42 a  " + "cross join ort42 o   "
                + " where k.addrId = a.addrID       " + "   and a.plz    = o.plz  "
                + " order by 1              ");

        // inner join on - wenn FK name nicht gleich PK name
        SqlTools.select(s, "select * from kund42 k " + "inner join addr42 a on (k.addrID = a.addrID) "
                + "inner join ort42  o on (a.plz = o.plz)       " + " order by 1              ");

        // inner join using - wenn FK name gleich PK name
        if (dbName.equals("H2") || dbName.equals("DB2") || dbName.equals("SQLSERVER"))
        {
            MyTools.uebOut(dbName + " macht Probleme bei INNER JOIN USING", 2);
        }
        else
        {
            SqlTools.select(s, "select * from kund42 k " + " join addr42 using(addrID)  "
                    + " join ort42  using(plz)     " + "   where k.name='huber'   "
                    + "      order by 1          ");
        }

        /*
		 * natural join - gleiche Attribute werden auf gleiche Daten geprueft
		 * 
		 * Achtung: wenn wir kz2 auf kz1 aendern, funktioniert der NATURAL JOIN nicht mehr.
		 * 
		 * Grund: jetzt sind die Attribute gleich aber die Daten unterschiedlich.
         */
        if (dbName.equals("H2") || dbName.equals("DB2") || dbName.equals("MYSQL")
                || dbName.equals("SQLSERVER"))
        {
            MyTools.uebOut(dbName + " macht Probleme bei NATURAL INNER JOIN", 2);
        }
        else
        {
            SqlTools.select(s, "select * from kund42 k           " + " natural inner join addr42  "
                    + " natural inner join ort42    ");
        }
    }

    static void a043(Statement s, String dbName)
    {
        MyTools.uebOut("***** a043 - outer join (left, right, full) " + "(on, using, natural]) *****", 2);

        SqlTools.executeUpdate(s, "drop table addr43", false);
        SqlTools.executeUpdate(s, "drop table kund43", false);
        SqlTools.executeUpdate(s, "create table kund43(id int,name varchar(20), " + "addrId int, kz1 int)");
        SqlTools.executeUpdate(s, "create table addr43(addrId int,ort varchar(20), kz2 int)");

        SqlTools.executeUpdate(s, "insert into addr43 values(1, 'basel'	, 2)");
        SqlTools.executeUpdate(s, "insert into addr43 values(2, 'bern'	, 2)");
        SqlTools.executeUpdate(s, "insert into addr43 values(3, 'paris'	, 2)");

        SqlTools.executeUpdate(s, "insert into kund43 values(1, 'huber' , 1,1)");
        SqlTools.executeUpdate(s, "insert into kund43 values(2, 'gerber', 2,1)");
        SqlTools.executeUpdate(s, "insert into kund43 values(6, 'berger', 4,1)");

        // left outer join on - wenn FK Name nicht gleich PK Name
        SqlTools.select(s, "select * from kund43 k " + " left outer join addr43 a on (k.addrId = a.addrId) order by 1");

        // right outer join on - wenn FK Name nicht gleich PK Name
        SqlTools.select(s, "select * from kund43 k " + " right outer join addr43 a on (k.addrId = a.addrId) order by 1");

        // left/right outer join using - nur wenn FK Name gleich PK Name
        if (dbName.equals("H2") || dbName.equals("DB2") || dbName.equals("SQLSERVER"))
        {
            MyTools.uebOut(dbName + " macht Probleme bei " + "left/right outer join USING", 2);
        }
        else
        {
            SqlTools.select(s, "select * from kund43 k  left  outer join addr43 a using (addrId) order by 1");
            SqlTools.select(s, "select * from kund43 k  right outer join addr43 a using (addrId) order by 1");
        }

        // full outer join ON
        if (dbName.equals("MYSQL") || dbName.equals("H2"))
        {
            MyTools.uebOut(dbName + " macht Probleme bei FULL outer join on", 2);
        }
        else
        {
            SqlTools.select(s, "select * from kund43 k " + "full outer join addr43 a on(k.addrId=a.addrId) order by 1");
        }

        // full outer join USING
        if (dbName.equals("MYSQL") || dbName.equals("H2") || dbName.equals("DB2") || dbName.equals("SQLSERVER"))
        {
            MyTools.uebOut(dbName + " macht Probleme bei FULL outer join USING", 2);
        }
        else
        {
            SqlTools.select(s, "select * from kund43 k full outer join addr43 a using(addrId) order by 1");
        }

        if (dbName.equals("SQLSERVER") || dbName.equals("MYSQL") || dbName.equals("H2") || dbName.equals("DB2"))
        {
            MyTools.uebOut(dbName + " macht Probleme bei " + "NATURAL left/right/full outer join", 2);
        }
        else
        {
            // natural left/right/full outer join
            SqlTools.select(s, "select * from kund43 k " + " natural left outer join addr43 a order by 1");
            SqlTools.select(s, "select * from kund43 k " + " natural right outer join addr43 a order by 1");
            SqlTools.select(s, "select * from kund43 k " + " natural full outer join addr43 a order by 1");
        }
    }

    static void a044(Statement s)
    {
        MyTools.uebOut("***** a044 - auto join *****", 2);

        SqlTools.executeUpdate(s, "drop table mitarbeiter", false);

        SqlTools.executeUpdate(s, "create table mitarbeiter(id int,name varchar(20), " + "vorgesetzterId int)");

        SqlTools.executeUpdate(s, "insert into mitarbeiter values(1, 'boss der bosse',null)");
        SqlTools.executeUpdate(s, "insert into mitarbeiter values(10, 'unterboss_1', 1)");
        SqlTools.executeUpdate(s, "insert into mitarbeiter values(20, 'unterboss_2', 1)");
        SqlTools.executeUpdate(s, "insert into mitarbeiter values(11, 'hugo'	, 10)");
        SqlTools.executeUpdate(s, "insert into mitarbeiter values(12, 'fritz'	, 10)");
        SqlTools.executeUpdate(s, "insert into mitarbeiter values(13, 'edi'	, 10)");
        SqlTools.executeUpdate(s, "insert into mitarbeiter values(21, 'max'	, 20)");
        SqlTools.executeUpdate(s, "insert into mitarbeiter values(22, 'moritz'	, 20)");

        SqlTools.select(s, "select * from mitarbeiter ");

        // auto join
        SqlTools.select(s, "select m.id , m.name MITARBEITER, v.name VORGESETZTER from mitarbeiter m, mitarbeiter v  where m.vorgesetzterId = v.id order by 1");

        // auto join als inner join
        SqlTools.select(s, "select m.id , m.name MITARBEITER, v.name VORGESETZTER from mitarbeiter m inner join  mitarbeiter v  on v.id=m.vorgesetzterId  order by 1");
    }
}
