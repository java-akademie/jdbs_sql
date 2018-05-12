package ch.jmildner.jdbc_sql.kapitel6;

import java.sql.Connection;
import java.sql.Statement;

import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import ch.jmildner.tools.SqlTools;

public class U6_Vertiefung1
{

    private static final String OS = "MAC";

    public static void main(String[] args) throws Exception
    {
        MyTools.uebOut("U6_Vertiefung1", 2);
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

            a06(dbName, s);
        }
        catch (Exception e)
        {
        }
    }

    static void a06(String dbName, Statement s)
    {
        a061(s, dbName);
        a062(s, dbName);
        a063(s);
        a064(s);
        a065(s, dbName);
        a066(s, dbName);
    }

    static void a061(Statement s, String dbName)
    {
        MyTools.uebOut("***** a061 - not null, unique, index *****", 2);

        if (dbName.equals("MYSQL") || dbName.equals("SQLSERVER"))
        {
            SqlTools.executeUpdate(s, "drop index pers61_name_ix on pers61");
            SqlTools.executeUpdate(s, "drop index pers61_kto_ix on pers61");
        }
        else
        {
            SqlTools.executeUpdate(s, "drop index pers61_name_ix");
            SqlTools.executeUpdate(s, "drop index pers61_kto_ix");
        }

        SqlTools.executeUpdate(s, "drop table pers61");

        String not_null = "";

        if (dbName.equals("DB2"))
        {
            not_null = "not null";
        }

        SqlTools.executeUpdate(s, "create table pers61(id int not null primary key"
                + ",name varchar(20), kto int " + not_null + ",kz int unique not null)");

        SqlTools.executeUpdate(s, "create        index pers61_name_ix on pers61(name)");
        SqlTools.executeUpdate(s, "create unique index pers61_kto_ix on pers61(kto)");

        SqlTools.executeUpdate(s, "insert into pers61 values(1, 'huber', 111,11)");
        SqlTools.executeUpdate(s, "insert into pers61 values(2, 'gerber', 112,12)");
        SqlTools.executeUpdate(s, "insert into pers61 values(5, 'berger', 223,21)");
        SqlTools.executeUpdate(s, "insert into pers61 values(6, null, 224,22)");
        SqlTools.executeUpdate(s, "insert into pers61 values(7, null, 225,31)");

        SqlTools.select(s, "select * from pers61");

    }

    static void a062(Statement s, String dbName)
    {
        MyTools.uebOut("***** a062 - DROP, CREATE PRIMARY KEY *****", 2);

        SqlTools.executeUpdate(s, "drop table pers62");

        SqlTools.executeUpdate(s, "create table pers62(id int not null" + ",name varchar(20)        "
                + ",CONSTRAINT pk_pers62 PRIMARY KEY(id))");

        SqlTools.executeUpdate(s, "insert into pers62 values(1, 'huber' )");
        SqlTools.executeUpdate(s, "insert into pers62 values(2, 'gerber')");
        SqlTools.executeUpdate(s, "insert into pers62 values(5, 'berger')");
        SqlTools.executeUpdate(s, "insert into pers62 values(6,  null   )");
        SqlTools.executeUpdate(s, "insert into pers62 values(7,  null   )");

        SqlTools.select(s, "select * from pers62");

        if (dbName.equals("MYSQL"))
        {
            SqlTools.executeUpdate(s, "alter table pers62 drop primary key");
        }
        else
        {
            SqlTools.executeUpdate(s, "alter table pers62 drop constraint pk_pers62");
        }

        SqlTools.executeUpdate(s, "update pers62 set id=id*10");

        SqlTools.executeUpdate(s, "alter table pers62 add constraint pk_pers62 primary key(id)");

        SqlTools.select(s, "select * from pers62");

        SqlTools.executeUpdate(s, "update pers62 set id=id*1000");

        SqlTools.select(s, "select * from pers62");
    }

    static void a063(Statement s)
    {
        MyTools.uebOut("***** a063 - " + "zusammengesetzte primary keys *****", 2);

        SqlTools.executeUpdate(s, "drop table besuch63");

        SqlTools.executeUpdate(s,
                "create table besuch63(       " + " veranstaltungsId   		int    not null    "
                + ",besucherId              int    not null    "
                + ",name               varchar(20)" + ",CONSTRAINT pk_besuch63 "
                + "       PRIMARY KEY(veranstaltungsId,besucherId))");

        SqlTools.executeUpdate(s, "insert into besuch63 values(1, 1 ,null)");
        SqlTools.executeUpdate(s, "insert into besuch63 values(1, 2 ,null)");
        SqlTools.executeUpdate(s, "insert into besuch63 values(1, 3 ,null)");
        SqlTools.executeUpdate(s, "insert into besuch63 values(2, 1 ,null)");
        SqlTools.executeUpdate(s, "insert into besuch63 values(2, 2 ,null)");

        SqlTools.select(s, "select * from besuch63");
    }

    static void a064(Statement s)
    {
        MyTools.uebOut("***** a064 - check *****", 2);

        SqlTools.executeUpdate(s, "drop table pers64");

        SqlTools.executeUpdate(s, "create table pers64(id int       " + ",name varchar(20)"
                + ",sex  char(1) check(sex in('m','f')) )");

        SqlTools.executeUpdate(s, "insert into pers64 values(1, 'fritz','m' )");
        SqlTools.executeUpdate(s, "insert into pers64 values(2, 'gerda','f' )");

        SqlTools.select(s, "select * from pers64");
    }

    static void a065(Statement s, String dbName)
    {
        MyTools.uebOut("***** a065 - ON DELETE / ON UPDATE "
                + "(POSTGRES, MYSQL, H2, SQLSERVER) *****", 2);

        if (dbName.startsWith("ORACLE") || dbName.equals("DB2"))
        {
            MyTools.uebOut(dbName + " ON UPDATE macht Probleme");
            return;
        }

        SqlTools.executeUpdate(s, "drop table pers65");
        SqlTools.executeUpdate(s, "drop table addr65");

        SqlTools.executeUpdate(s, "create table addr65 (id int not null primary key, ort varchar(20))");

        SqlTools.executeUpdate(s,
                "create table pers65 " //
                + "("//
                + " id     int not null primary key" //
                + ",name varchar(20)" //
                + ",addrId int not null" //
                + ",CONSTRAINT fk_pers65 FOREIGN KEY (addrId) REFERENCES addr65(id)"
                + "  ON DELETE CASCADE  ON UPDATE CASCADE" //
                + ")"//
        );

        SqlTools.executeUpdate(s, "insert into addr65 values(1, 'BASEL')");
        SqlTools.executeUpdate(s, "insert into addr65 values(2, 'GENF')");
        SqlTools.executeUpdate(s, "insert into addr65 values(3, 'BERN')");

        SqlTools.executeUpdate(s, "insert into pers65  values(1, 'HUBER' ,1)");
        SqlTools.executeUpdate(s, "insert into pers65  values(2, 'MEIER' ,2)");
        SqlTools.executeUpdate(s, "insert into pers65  values(3, 'GRUBER' ,3)");

        SqlTools.select(s, "select * from pers65");
        SqlTools.select(s, "select * from addr65");

        SqlTools.executeUpdate(s, "delete from addr65 where id=1");
        SqlTools.executeUpdate(s, "update addr65 set id=12 where id=2");

        SqlTools.select(s, "select * from pers65");
        SqlTools.select(s, "select * from addr65");

    }

    static void a066(Statement s, String dbName)
    {
        MyTools.uebOut("***** a066 - ON DELETE / kein ON UPDATE"
                + "(DB2,ORACLE macht bei ON UPDATE Probleme) *****", 2);

        SqlTools.executeUpdate(s, "drop table pers66");
        SqlTools.executeUpdate(s, "drop table addr66");

        SqlTools.executeUpdate(s,
                "create table addr66 (id int not null primary key " + ",ort  varchar(20))");

        SqlTools.executeUpdate(s, "create table pers66(id int not null primary key "
                + ",name varchar(20)            " + ",addrId int not null)        ");

        SqlTools.executeUpdate(s, "alter table pers66 add  constraint fk_addr66"
                + " foreign key(addrId) references addr66(id) " + "    on delete cascade");

        SqlTools.executeUpdate(s, "insert into addr66 values(1, 'BASEL')");
        SqlTools.executeUpdate(s, "insert into addr66 values(2, 'GENF')");
        SqlTools.executeUpdate(s, "insert into addr66 values(3, 'BERN')");

        SqlTools.executeUpdate(s, "insert into pers66  values(1, 'HUBER' ,1)");
        SqlTools.executeUpdate(s, "insert into pers66  values(2, 'MEIER' ,2)");
        SqlTools.executeUpdate(s, "insert into pers66  values(3, 'GRUBER' ,3)");

        SqlTools.select(s, "select * from pers66");
        SqlTools.select(s, "select * from addr66");

        SqlTools.executeUpdate(s, "delete from addr66 where id=1");

        SqlTools.select(s, "select * from pers66");
        SqlTools.select(s, "select * from addr66");

    }

}
