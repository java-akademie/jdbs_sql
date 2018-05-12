package ch.jmildner.jdbc_sql.kapitel6;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;

public class U6_Vertiefung2
{

    private static final String OS = "MAC";

    public static void main(String[] args) throws Exception
    {
        MyTools.uebOut("U&_Vertiefung2", 2);
        // doit("DB2");
        // doit("ORACLE_EUROPA");
        // doit("SQLSERVER");
        // doit("POSTGRES");
         doit("MYSQL");
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

            a07(dbName, s);
        }
        catch (Exception e)
        {
        }
    }

    static void a07(String dbName, Statement s)
    {
        a071(s);
        a072(s);
        a073(s);
        a074(s);
    }

    static void a071(Statement s)
    {
        MyTools.uebOut("******* a071 - WHERE (like/<>/=/!==) *******",
                2);

        SqlTools.executeUpdate(s, "drop table kund71");

        SqlTools.executeUpdate(s,
                "create table kund71(id int not null primary key "
                + ",name varchar(20)  "
                + ",x varchar(10) "
                + ",y varchar(10) )");

        SqlTools.executeUpdate(s,
                "insert into kund71 values(1, 'hu\\ber','a','b' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(2, 'gruber','a','b' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(3, 'meier'           ,'a','b' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(4, 'huber'           ,'a','b' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(5, 'wirtz%xx'        ,'a','c' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(6, 'obermeier_aa'    ,'a','b' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(7, 'meier\\beer'     ,'a','c' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(8, 'mister-10%xxx x' ,'a','b' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(9, 'huber'           ,'a','c' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(10,'geier'           ,'a','c' )");
        SqlTools.executeUpdate(s,
                "insert into kund71 values(11,'eier'            ,'a','c' )");

        SqlTools.select(s,
                "select distinct 'KZ:' || x as KZ from kund71");
        SqlTools.select(s, "select distinct y as KZ from kund71");

        SqlTools.select(s, "select * from kund71 order by 2");

        // -- where bedingungen
        // -- =, >=, <=
        // -- <> nicht gleich, != nicht gleich
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name =  'huber'");
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name <> 'huber'");
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name != 'huber'");
        SqlTools.select(s, "SELECT * FROM kund71 WHERE name >= 'ober'");
        SqlTools.select(s, "SELECT * FROM kund71 WHERE name <= 'ober'");

        // -- where bedingungen
        // -- like, not like
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name     like '%meier%'");
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name not like '%meier%'");

        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name like '%!%%'  escape '!' ");
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name like '%*_%'  escape '*' ");
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name like '%!\\%' escape '!' ");

        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name like '_eier'  ");
        SqlTools.select(s,
                "SELECT * FROM kund71 WHERE name like '%eier'  ");
    }

    static void a072(Statement s)
    {
        MyTools.uebOut(
                "******* a072 - "
                + "Gruppenfunktionen (COUNT,SUM,MIN,MAX,AVG) *******",
                2);

        SqlTools.executeUpdate(s, "drop table mitarb72");

        SqlTools.executeUpdate(s,
                "create table mitarb72(id int not null primary key "
                + ",name varchar(20)" + ",salaer int)");

        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(1,  'huber'    ,  null)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(2,  'gruber'   ,  333)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(3,  'meier'    ,  400)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(4,  'huber'    ,  200)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(5,  'wirtz'    ,  123)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(6,  'obermeier',  122)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(7,  'meierbeer',  122)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(8,  'huber'    , 1234)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(9,  'huber'    , 1234)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(10, 'huber'    ,  223)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(11, 'huber'    ,23445)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(12, 'huber'    ,  555)");
        SqlTools.executeUpdate(s,
                "insert into mitarb72 values(13, 'huber'    , null)");

        SqlTools.select(s, "SELECT * FROM mitarb72");

        SqlTools.select(s,
                "SELECT " + "   count(*), count(salaer), min(salaer), "
                + " max(salaer), round(avg(salaer),2), sum(salaer)"
                + " FROM mitarb72");
    }

    static void a073(Statement s)
    {
        MyTools.uebOut(
                "******* a073 - " + "GROUP BY ... HAVING *******", 2);

        SqlTools.executeUpdate(s, "drop table mitarb73");

        SqlTools.executeUpdate(s,
                "create table mitarb73(id int not null primary key "
                + ",name varchar(20) "
                + ",abtId  int "
                + ",salaer int )");

        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(1,  'huber'    ,1, null)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(2,  'gruber'   ,1,  333)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(3,  'meier'    ,1,  400)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(4,  'huber'    ,2,  200)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(5,  'wirtz'    ,2,  123)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(6,  'obermeier',2,  122)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(7,  'meierbeer',2,  122)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(8,  'huber'    ,2, 1234)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(9,  'huber'    ,3, 1234)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(10, 'huber'    ,3,  223)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(11, 'huber'    ,4,23445)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(12, 'huber'    ,5, null)");
        SqlTools.executeUpdate(s,
                "insert into mitarb73 values(13, 'huber'    ,5, null)");

        SqlTools.select(s, "SELECT * FROM mitarb73");

        SqlTools.select(s,
                "SELECT abtId, count(*), count(salaer), min(salaer), "
                + " max(salaer), round(avg(salaer),2), sum(salaer)  "
                + " FROM mitarb73 GROUP BY abtId");

        SqlTools.select(s,
                "SELECT abtId, count(*), count(salaer), min(salaer),         "
                + " max(salaer), round(avg(salaer),2), sum(salaer)  "
                + " FROM mitarb73 GROUP BY abtId  HAVING min(salaer) > 250");

    }

    static void a074(Statement s)
    {
        MyTools.uebOut("******* a074 - SUBQUERIES *******", 2);

        SqlTools.executeUpdate(s, "drop table maenner74");
        SqlTools.executeUpdate(s, "drop table frauen74");

        SqlTools.executeUpdate(s,
                "create table maenner74(id int not null primary key "
                + ", name varchar(20), gewicht int)");

        SqlTools.executeUpdate(s,
                "create table frauen74(id int not null primary key "
                + ", name varchar(20), gewicht int)");

        SqlTools.executeUpdate(s,
                "insert into maenner74 values(1, 'fritz',70)");
        SqlTools.executeUpdate(s,
                "insert into maenner74 values(2, 'franz',75)");
        SqlTools.executeUpdate(s,
                "insert into maenner74 values(3, 'urs',  80)");
        SqlTools.executeUpdate(s,
                "insert into maenner74 values(13, 'urs2',80)");
        SqlTools.executeUpdate(s,
                "insert into maenner74 values(4, 'gerd', 85)");
        SqlTools.executeUpdate(s,
                "insert into maenner74 values(5, 'hans', 90)");
        SqlTools.executeUpdate(s,
                "insert into maenner74 values(6, 'peter',95)");

        SqlTools.executeUpdate(s,
                "insert into frauen74 values(1, 'eva',    45)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(2, 'sandra', 50)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(3, 'petra',  55)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(4, 'eveline',60)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(5, 'julia',  65)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(6, 'erika',  70)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(7, 'gerda',  75)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(8, 'roberta',80)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(9, 'linda',  85)");
        SqlTools.executeUpdate(s,
                "insert into frauen74 values(10, 'hertha',90)");

        SqlTools.select(s, "SELECT * FROM maenner74");
        SqlTools.select(s, "SELECT * FROM frauen74");

        SqlTools.select(s,
                "select round(avg(gewicht),2)  from maenner74");
        SqlTools.select(s,
                "select round(avg(gewicht),2)  from frauen74");

        SqlTools.select(s, "select * from frauen74 where gewicht > "
                + "(select avg(gewicht) from maenner74)");

        SqlTools.select(s, "select * from frauen74 where gewicht < "
                + "(select avg(gewicht) from maenner74)");

        SqlTools.select(s, "select * from frauen74 f where gewicht in "
                + "(select gewicht from maenner74 m  where m.gewicht=f.gewicht)");

        SqlTools.select(s,
                "select * from frauen74 f where gewicht between "
                + " (select avg(gewicht) from frauen74) "
                + "and (select avg(gewicht) from maenner74)");
    }

}
