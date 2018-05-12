package ch.jmildner.jdbc_sql.kapitel3;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;
import java.sql.SQLException;

public class U03_RelationaleAlgebra
{

    private static final String OS = "MAC";

    public static void main(String[] args)
    {
        doit("ORACLE");
        doit("POSTGRES");
        doit("DB2");
        doit("MYSQL");
        doit("H2");
    }

    static void doit(String dbName)
    {
        MyTools.uebOut("Start U03_RelationaleAlgebra doit() with DB " + dbName, 3);
        
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
                a03(dbName, c);
            }
            catch (Exception e)
            {
                System.out.println("------------ EXCEPTION ------------");
                System.out.println(e.getMessage());
                System.out.println("------------ EXCEPTION ------------");

                MyTools.pause();
            }
        }

        MyTools.untOut("Stopp U03_RelationaleAlgebra doit() with DB " + dbName, 3);

    }

    static void a03(String dbName, Connection c) throws SQLException
    {
        try (Statement s = c.createStatement())
        {
            a031(s);
            a032(s);
            a033(s);
            a034(s, dbName);
            a035(s, dbName);
        }
    }

    static void a031(Statement s)
    {
        MyTools.uebOut("***** a031 - (restriction/projection/alias) *****", 2);

        SqlTools.executeUpdate(s, "drop table pers31", false);
        SqlTools.executeUpdate(s, "create table pers31 (id int not null primary key,name varchar(20),ort varchar(20))");

        SqlTools.executeUpdate(s, "insert into pers31 values(1, 'huber'  , 'basel' )");
        SqlTools.executeUpdate(s, "insert into pers31 values(2, 'gruber' , 'genf'  )");
        SqlTools.executeUpdate(s, "insert into pers31 values(3, 'meier'  , 'bern'  )");
        SqlTools.executeUpdate(s, "insert into pers31 values(4, 'scholl' , 'bruck' )");
        SqlTools.executeUpdate(s, "insert into pers31 values(5, 'banz'   , 'baden' )");
        SqlTools.executeUpdate(s, "insert into pers31 values(6, 'benz'   , 'bern'  )");

        // selection
        SqlTools.select(s, "select * from pers31");

        // restriction
        SqlTools.select(s, "select * from pers31 where ort='basel'");

        // projektion
        SqlTools.select(s, "select name, id from pers31");

        // projektion & restriction
        SqlTools.select(s, "select name, id from pers31 where ort='basel'");

        // alias
        SqlTools.select(s, "select p.name nachname, p.id persnr from pers31 p");
    }

    static void a032(Statement s)
    {
        MyTools.uebOut("***** a032 - (kartesische Produkt) *****", 2);

        SqlTools.executeUpdate(s, "drop table pers32", false);
        SqlTools.executeUpdate(s, "drop table ort", false);
        SqlTools.executeUpdate(s, "create table pers32 (id int" + ",name varchar(20), ortId int)");
        SqlTools.executeUpdate(s, "create table ort (id int" + ",ort varchar(20))");

        SqlTools.executeUpdate(s, "insert into pers32 values(1, 'huber'  , 100)");
        SqlTools.executeUpdate(s, "insert into pers32 values(2, 'gruber' , 200)");
        SqlTools.executeUpdate(s, "insert into pers32 values(3, 'weiss'  , 200)");
        SqlTools.executeUpdate(s, "insert into ort values(100, 'basel')");
        SqlTools.executeUpdate(s, "insert into ort values(200, 'bern' )");

        // kartesisches produkt
        SqlTools.select(s, "select * from pers32, ort");

    }

    static void a033(Statement s)
    {
        MyTools.uebOut("***** a033 - (union - Vereinigung) *****", 2);

        SqlTools.executeUpdate(s, "drop table pers33", false);
        SqlTools.executeUpdate(s, "drop table kund33", false);
        SqlTools.executeUpdate(s, "drop table lief33", false);
        SqlTools.executeUpdate(s, "create table pers33 (id int,name varchar(20),ort varchar(20), salaer decimal(31,2))");
        SqlTools.executeUpdate(s, "create table kund33 (id int,firmenname varchar(22),addr varchar(22), kont decimal(20,2))");
        SqlTools.executeUpdate(s, "create table lief33 (id int" + ",lname varchar(22), lort varchar(22), lbet int)");

        SqlTools.executeUpdate(s, "insert into pers33 values(1, 'huber' , 'basel',155312.50)");
        SqlTools.executeUpdate(s, "insert into pers33 values(2, 'meier' , 'genf', 155432.50)");
        SqlTools.executeUpdate(s, "insert into pers33 values(3, 'banu'  , 'bern', 213312.50)");

        SqlTools.executeUpdate(s, "insert into kund33 values(1,'huber und Sohn','brugg', 5.35)");
        SqlTools.executeUpdate(s, "insert into kund33 values(3,'gruber KG','baden',15.35)");
        SqlTools.executeUpdate(s, "insert into kund33 values(3,'benu AG','wien',25.35)");

        SqlTools.executeUpdate(s, "insert into lief33 values(1,'geier KG','basel',995)");
        SqlTools.executeUpdate(s, "insert into lief33 values(2,'leier GmbH','basel',124)");
        SqlTools.executeUpdate(s, "insert into lief33 values(3,'schulz OHG','basel',312)");

        // union
        SqlTools.select(s, "select 'P' kennzeichen,name pers33enname,ort adresse, salaer betrag from pers33 union select 'K',firmenname,addr,kont from kund33 union select 'L',lname,lort,lbet from lief33");

        // union mit select *
        // tabellenueberschriften werden aus dem ersten select
        // genommen
        // die Anzahl der Attribute muss gleich sein
        // mysql: Typ der Attribute kann unterschiedlich sein
        // oracle: Typ der Attribute muss gleich sein
        SqlTools.select(s, "select * from pers33 union " + "select * from kund33 union select * from lief33 ");
    }

    static void a034(Statement s, String dbName)
    {
        MyTools.uebOut("***** a034 - (intersect - Schnittmenge) *****");

        if (dbName.equals("MYSQL"))
        {
            System.out.println("MYSQL kennt kein intersect");
            return;
        }

        SqlTools.executeUpdate(s, "drop table pers34", false);
        SqlTools.executeUpdate(s, "drop table kund34", false);
        SqlTools.executeUpdate(s, "create table pers34 (id int,name varchar(20), ort varchar(20), salaer decimal(31,2))");
        SqlTools.executeUpdate(s, "create table kund34 (id int,firmenname varchar(22), addr varchar(22), kont decimal(20,2))");

        SqlTools.executeUpdate(s, "insert into pers34 values(1, 'huber'  , 'basel', 111.50)");
        SqlTools.executeUpdate(s, "insert into pers34 values(2, 'meier'  , 'genf' , 112.50)");
        SqlTools.executeUpdate(s, "insert into pers34 values(3, 'banz'   , 'bern' , 222.50)");
        SqlTools.executeUpdate(s, "insert into pers34 values(4, 'huber'  , 'basel', 333.50)");
        SqlTools.executeUpdate(s, "insert into pers34 values(5, 'huber'  , 'basel', 444.50)");

        SqlTools.executeUpdate(s, "insert into kund34 values(1, 'huber'  , 'basel', 277.35)");
        SqlTools.executeUpdate(s, "insert into kund34 values(2, 'huber'  , 'basel', 277.35)");
        SqlTools.executeUpdate(s, "insert into kund34 values(3, 'huber'  , 'basel', 277.35)");
        SqlTools.executeUpdate(s, "insert into kund34 values(4, 'gruber' , 'baden', 288.35)");
        SqlTools.executeUpdate(s, "insert into kund34 values(5, 'benu AG', 'wien' , 444.35)");

        // intersect
        SqlTools.select(s, "select * from pers34 where salaer between 1 and 250 intersect select * from pers34 where salaer between 200 and 500 ");

        // intersect
        SqlTools.select(s, "select name,ort from pers34 " + "intersect select firmenname,addr from kund34 ");

        // intersect all
        String all = "all";

        if (dbName.equals("H2") || dbName.startsWith("ORACLE") || dbName.equals("SQLSERVER"))
        {
            all = ""; // intersect all bei H2 und ORACLE fehlerhaft
            System.out.println(dbName + " bringt bei ALL in INTERSECT einen Fehler");

        }
        SqlTools.select(s, "select name,ort from pers34 " + "intersect  " + all + " select firmenname,addr from kund34 ");

    }

    static void a035(Statement s, String dbName)
    {

        MyTools.uebOut("***** a035 - (except/minus - Differenz) *****", 2);

        if (dbName.equals("MYSQL"))
        {
            System.out.println("MYSQL kennt kein except/minus");
            return;
        }

        SqlTools.executeUpdate(s, "drop table pers35", false);
        SqlTools.executeUpdate(s, "drop table kund35", false);
        SqlTools.executeUpdate(s, "create table pers35 (id int,name varchar(20), ort varchar(20), salaer decimal(31,2))");
        SqlTools.executeUpdate(s, "create table kund35 (id int,firmenname varchar(22), addr varchar(22),kont decimal(20,2))");

        SqlTools.executeUpdate(s, "insert into pers35 values(1, 'huber'  , 'basel', 111.50)");
        SqlTools.executeUpdate(s, "insert into pers35 values(2, 'meier'  , 'genf' , 112.50)");
        SqlTools.executeUpdate(s, "insert into pers35 values(3, 'banz'   , 'bern' , 222.50)");
        SqlTools.executeUpdate(s, "insert into pers35 values(4, 'huber'  , 'basel', 333.50)");
        SqlTools.executeUpdate(s, "insert into pers35 values(5, 'huber'  , 'basel', 444.50)");

        SqlTools.executeUpdate(s, "insert into kund35 values(1, 'huber'  , 'basel', 277.35)");
        SqlTools.executeUpdate(s, "insert into kund35 values(2, 'huber'  , 'basel', 277.35)");
        SqlTools.executeUpdate(s, "insert into kund35 values(3, 'huber'  , 'basel', 277.35)");
        SqlTools.executeUpdate(s, "insert into kund35 values(4, 'gruber ', 'baden', 288.35)");
        SqlTools.executeUpdate(s, "insert into kund35 values(5, 'benu AG', 'wien' , 444.35)");

        SqlTools.select(s, "select * from pers35 ");
        SqlTools.select(s, "select * from kund35 ");

        // except/minus
        String except = "except";

        if (dbName.startsWith("ORACLE"))
        {
            except = "minus";
        }
        SqlTools.select(s, "select name,ort from pers35 " + except + " select firmenname,addr from kund35 ");
    }

}
