package ch.jmildner.jdbc_sql.sontiges;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ShowDatabaseDetails
{

    public static void main(String[] args)
    {
        new ShowDatabaseDetails().run();
    }

    public void run() 
    {
        // run("MYSQL");
        // run("POSTGRES");
        //run("ORACLE");
        // run("SQLSERVER");
        run("H2");
        // run("DB2");
    }

    public void run(String db)
    {

        displayDatabaseDetails(db);

    }

    public void displayDatabaseDetails(String db)
    {
        try (Connection con = new MyDataSource(db).getConnection())
        {
            DatabaseMetaData dbMetaData = con.getMetaData();

            dbInformation(dbMetaData);
            catalogs(dbMetaData);
            schemas(dbMetaData);
            tableTypes(dbMetaData);
            tables(dbMetaData);

            isolationLevel(con, dbMetaData);
        }
        catch (Exception ex)
        {
            Logger.getLogger(ShowDatabaseDetails.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void tables(DatabaseMetaData dm)
    {
        MyTools.uebOut("Tables", 2);

        // boolean debug = false;
        // if (!debug) return;
        try
        {
            // getTables(catalog,schemaPattern,tableNamePattern,types)

            ResultSet rs;

            if (dm.getDatabaseProductName().equals("Oracle"))
            {
                rs = dm.getTables(null, "JEES", null, null);
            }
            else if (dm.getDatabaseProductName().equals("H2"))
            {
                rs = dm.getTables(null, "PUBLIC", null, null);
            }
            else if (dm.getDatabaseProductName().equals("MySQL"))
            {
                rs = dm.getTables(null, null, null, null);
            }
            else if (dm.getDatabaseProductName()
                    .equals("PostgreSQL"))
            {
                rs = dm.getTables(null, "public", null,
                        null);
            }
            else
            {
                rs = dm.getTables(null, null, null, null);
            }

            ResultSetMetaData md = rs.getMetaData();

            int anz = md.getColumnCount();

            for (int i = 1; i <= anz; i++)
            {
                System.out.printf("%-40S ", md.getColumnLabel(i));
            }

            System.out.println();

            while (rs.next())
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= anz; i++)
                {
                    try
                    {
                        if (i < anz)
                        {
                            String s = String.format("%-40s ",
                                    rs.getString(i));
                            sb.append(s);
                        }
                        else
                        {
                            // if(db.equals("ORACLE")) continue;
                            // String s = String.format("%-333s ",
                            // rs.getString(i));
                            // sb.append(s);
                        }
                    }
                    catch (Exception e)
                    {
                        // System.out.println(e);
                        sb.append(" ... " + i);
                        break;
                    }
                }
                info(sb.toString());
            }

            rs.close();
        }
        catch (Exception e)
        {
        }
    }

    private void tableTypes(DatabaseMetaData dm) throws SQLException
    {

        MyTools.uebOut("TableTypes", 2);

        ResultSet rst = dm.getTableTypes();

        while (rst.next())
        {
            info("\tTableType: " + rst.getString(1));
        }

        rst.close();
    }

    private void schemas(DatabaseMetaData dm) throws SQLException
    {
        MyTools.uebOut("Schemas", 2);

        ResultSet rss = dm.getSchemas();

        while (rss.next())
        {
            info("\tSchema: " + rss.getString(1));
        }

        rss.close();
    }

    private void catalogs(DatabaseMetaData dm) throws SQLException
    {
        MyTools.uebOut("Catalogs", 2);

        ResultSet rsc = dm.getCatalogs();

        while (rsc.next())
        {
            info("\tCatalog: " + rsc.getString(1));
        }

        rsc.close();

    }

    private void dbInformation(DatabaseMetaData dm) throws SQLException
    {
        MyTools.uebOut("db Information - Driver", 2);
        info("\tDriver Name:      " + dm.getDriverName());
        info("\tDriver Version:   " + dm.getDriverVersion());

        MyTools.uebOut("db Information - Database", 2);
        info("\tDatabase Name:    " + dm.getDatabaseProductName());
        info("\tDatabase Version: " + dm.getDatabaseProductVersion());
    }

    private void isolationLevel(Connection con, DatabaseMetaData dm)
            throws SQLException
    {
        MyTools.uebOut("Transaction Isolation Level", 2);

        for (int i = 0; i <= 4; i++)
        {
            int ix = i == 0 ? 0 : (int) (Math.pow(2, i - 1));

            String isolationLevelString = getIsolationLevelString(ix);

            if (dm.supportsTransactionIsolationLevel(ix))
            {
                info("\tTransaction supports Isolation Level " + ix
                        + " - " + isolationLevelString);
            }
        }

        int aktiverIsolationLevel = con.getTransactionIsolation();

        info("\n\tAktiver Isolation Level " + aktiverIsolationLevel
                + " - "
                + getIsolationLevelString(aktiverIsolationLevel));
    }

    private String getIsolationLevelString(int i)
    {
        switch (i)
        {
            case 0:
                return "TRANSACTION_NONE";
            case 1:
                return "TRANSACTION_READ_UNCOMMITTED";
            case 2:
                return "TRANSACTION_READ_COMMITTED";
            case 4:
                return "TRANSACTION_REPEATABLE_READ";
            case 8:
                return "TRANSACTION_SERIALIZABLE";
            default:
                return "???";
        }
    }

    private void info(String s)
    {
        System.out.println(s);
    }
}
