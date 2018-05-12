package ch.jmildner.jdbc_sql.resultPager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultPager
{
	private int page;
	private long results;
	private int resultsPerPage;


	// public ResultPager(int resultPerPage, Query countQuery)
	// {
	// this.resultsPerPage = resultPerPage;
	// results = (Long) countQuery.getSingleResult();
	// }


	public ResultPager(int resultPerPage, ResultSet rs)
			throws SQLException
	{
		this.resultsPerPage = resultPerPage;

		while (rs.next())
		{
			this.results = rs.getInt(1);
			break;
		}

		System.out.println("ANZAHL ZEILEN: " + results);
	}


	public long getPages()
	{
		long pages = results / resultsPerPage;

		if (results % resultsPerPage != 0)
			pages++;

		return pages;
	}


	public List<?> getResultsOfPage(ResultSet rs) throws SQLException
	{
		System.out.println(222 + "-----------------");
		List<String> list = new ArrayList<String>();
		int zz = 0;

		while (rs.next())
		{
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int wert = rs.getInt("wert");
			String zeile = String.format("%5d %10d %s", id, wert, name);
			list.add(zeile);
			zz++;

			if (zz == 10)
				break;
		}

		return list;
	}


	// public List<?> getResultsOfPage(Query query)
	// {
	// System.out.println(111 + "-----------------");
	// return query.setFirstResult(page * resultsPerPage)
	// .setMaxResults(resultsPerPage).getResultList();
	// }


	public void next()
	{
		page++;
	}


	public void back()
	{
		if (--page < 0)
			page = 0;
	}
}

