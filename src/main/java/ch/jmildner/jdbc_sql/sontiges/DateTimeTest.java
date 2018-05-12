package ch.jmildner.jdbc_sql.sontiges;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import ch.jmildner.tools.DateTimeTools;
import ch.jmildner.tools.MyDataSource;
import ch.jmildner.tools.MyTools;

public class DateTimeTest
{
	private static final String DB = "H2";


	public static void main(String[] args) throws Exception
	{
		MyTools.uebOut("Start DateTimeTest");

		test1();
		test2();

		MyTools.untOut("Stopp DateTimeTest");
	}


	private static void test2() throws Exception
	{
		MyTools.uebOut("Start Test2 (DateTime Tool)", 1);

		Date dt = DateTimeTools.getCurrentDate();
		Time zt = DateTimeTools.getCurrentTime();
		Timestamp ts = DateTimeTools.getCurrentTimestamp();
		long datum_ms = dt.getTime();
		long zeit_ms = zt.getTime();

		System.out.println(dt + "\t" + zt + "\t" + ts + "\n");

		Timestamp datum_ts = new Timestamp(datum_ms);
		System.out.println("Datum: " + datum_ts);

		Timestamp zeit_ts = new Timestamp(zeit_ms);
		System.out.println("Zeit : " + zeit_ts);



		MyTools.untOut("Stopp Test2", 1);
	}


	private static void test1() throws Exception
	{
		MyTools.uebOut("Start Test1 (DB)", 1);

		Connection c = new MyDataSource(DB).getConnection();

		Statement s = c.createStatement();

		try
		{
			s.execute("drop table ddtest");
		}
		catch (Exception e)
		{
		}

		s.execute("create table ddtest (id int primary key,"
				+ " datum date, zeit time, zeitstempel timestamp)");

		s.execute("insert into ddtest "
				+ "values(1,current_date,current_time,current_timestamp)");

		s.execute("select id,datum,zeit,zeitstempel from ddtest");

		ResultSet rs = s.getResultSet();

		while (rs.next())
		{
			Date dt = rs.getDate("datum");
			Time zt = rs.getTime("zeit");
			Timestamp ts = rs.getTimestamp("zeitstempel");
			long datum_ms = dt.getTime();
			long zeit_ms = zt.getTime();

			System.out.println(dt + "\t" + zt + "\t" + ts + "\n");

			Timestamp datum_ts = new Timestamp(datum_ms);
			System.out.println("Datum: " + datum_ts);

			Timestamp zeit_ts = new Timestamp(zeit_ms);
			System.out.println("Zeit : " + zeit_ts);

			testTimestamps();

		}

		rs.close();

		c.close();

		MyTools.untOut("Stopp Test1", 1);
	}


	private static void testTimestamps()
	{
		// long jahr = 1000L * 60 * 60 * 24 * 365;
		//
		// for (int i = 1600; i <= 2000; i++)
		// {
		// Timestamp zw = new Timestamp(-1L * i * jahr);
		// System.out.println(i + " : " + zw);
		// }
	}
}
