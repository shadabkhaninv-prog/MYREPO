package bhav;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.ParseException;

public class MomentumSimulator {
	// Get list of the stocks for the month

	private Connection connection;
	ArrayList<String> mktdates = null;
	HashSet<Ticker> trades = new HashSet<Ticker>();

	private static final int stoplossLevel = 5;
	private java.util.Date currentDay;

	private static final double trailstoplossLevel = 50;
	private static double totalCapital=300000;
	
	private int countOfStocks=20;
	
	private boolean firstRun=true;
	
	ArrayList<Ticker> alltrades = new ArrayList<Ticker>();
	private double averageLookBackRoc=0;
	
	double avgLookbackroc;
	double avgcurrentroc;
	double momentumdelta;

	//select a.symbol,a.buyprice,b.close,((b.CLOSE-a.buyprice)/b.CLOSE)*100 AS ROC from portfolioJune2019 a,bhav2019 b where a.symbol=b.symbol and b.mktdate='2019-07-31'

	
	private void updateForBonusandsplits(String portfolio,String firstday,String lastday) throws SQLException,Exception{
		String query="update "+portfolio+" a inner join splits b on a.symbol=b.symbol set a.soldprice=a.soldprice*b.ratio,roc=((a.soldprice-a.buyprice)/a.soldprice)*100"
				+ " where b.exdate>=? and b.exdate<=?;";
		String query2="update "+portfolio+" a inner join bonus b on a.symbol=b.symbol set a.soldprice=a.soldprice*b.ratio,roc=((a.soldprice-a.buyprice)/a.soldprice)*100"
				+ " where b.exdate>=? and b.exdate<=?;";
		PreparedStatement selQuery;
		try {
			selQuery = connection.prepareStatement(query);
			

			selQuery.setString(1, firstday);
			selQuery.setString(2, lastday);
			int rs = selQuery.executeUpdate();
			
selQuery = connection.prepareStatement(query2);
			

			selQuery.setString(1, firstday);
			selQuery.setString(2, lastday);
			 rs = selQuery.executeUpdate();

		}catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		
	}
	
private void calculatePerformance(String month,String year,String startdate) throws SQLException,Exception{
	String bhavName = "bhav" + year;
	if(month.equalsIgnoreCase("DECEMBER")){
		int iyear=Integer.parseInt(year)+1;
		bhavName = "bhav" + iyear;
	}
	
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

	SimpleDateFormat df = new SimpleDateFormat("yyyy");
	java.util.Date date = format.parse(startdate);
	String portfolio="portfolio"+month+year;
	String query="update portfolio"+month+year+" a inner join "+bhavName+" b on a.symbol=b.symbol and b.mktdate=? set roc=((b.CLOSE-a.buyprice)/a.buyprice)*100,soldprice=b.close";
	String liquidquery="update portfolio"+month+year+" a set soldprice=buyprice+(buyprice*0.6/100) where symbol='Liquid'";
	
	String queryforzeroes="update portfolio"+month+year+" set roc=((soldprice-buyprice)/buyprice)*100,soldprice=buyprice where soldprice=0";

	PreparedStatement selQuery;
	PreparedStatement createQuery;


	Calendar cal = Calendar.getInstance();
	cal.setTime(date);

	//cal.add(Calendar.MONTH, 1);
	cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));

	String lastday = getLastDayOfTheMonth(format.format(cal.getTime()));
	cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
	String firstday=getFirstDayOfTheMonth(format.format(cal.getTime()));

	// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
	// on "+ticker.getMktdate());
	try {
		selQuery = connection.prepareStatement(query);
		

		selQuery.setString(1, lastday);
		
		
		
		selQuery.executeUpdate();
		
		selQuery = connection.prepareStatement(queryforzeroes);		
		 selQuery.executeUpdate();
		
		selQuery = connection.prepareStatement(liquidquery);
	 selQuery.executeUpdate();

		
		updateForBonusandsplits(portfolio,firstday,lastday);
		
		Ticker ticker = null;
		
		Statement st = connection.createStatement();
		st.execute("insert into consolidatedportfolio select *,'"+month+year+"' from "+portfolio);
		updatePortfolioTracker(portfolio, lastday);
		
		
	} catch (SQLException e) {
		e.printStackTrace();
		throw e;
	}
 
}
/**
 * INSERT INTO `bhav`.`portfoliotracker`
(`AsOf`,
`startingcorpus`,
`endingcorpus`,
`lookbackroc`,
`currentroc`,
`monthroc`,
`circuithit`,
`circuitdate`)
VALUES
(<{AsOf: }>,
<{startingcorpus: }>,
<{endingcorpus: }>,
<{lookbackroc: }>,
<{currentroc: }>,
<{monthroc: }>,
<{circuithit: }>,
<{circuitdate: }>);
 */
private void updatePortfolioTracker(String month,String mktdate)throws Exception{
	String query="insert into portfoliotracker select ?,?,sum(encashed),0,?,?,? from("
			+ "select *,(buyqty*buyprice) as invested,(buyqty*soldprice) as encashed from "+month+") a;";
	PreparedStatement selQuery;


	// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
	// on "+ticker.getMktdate());
	try {
		selQuery = connection.prepareStatement(query);
		selQuery.setString(1, mktdate);
		selQuery.setDouble(2, totalCapital);

		selQuery.setDouble(3, this.averageLookBackRoc);
		selQuery.setDouble(4, this.avgcurrentroc);
		selQuery.setDouble(5, this.momentumdelta);

		
		int update=selQuery.executeUpdate();
	}catch (SQLException e) {
		e.printStackTrace();
		throw e;
	}
}

private void allocatePortfolio(String month,String year,String closingdate,double momentumdelta)throws Exception{
	String bhavName = "bhav" + year;
//	if(month.equalsIgnoreCase("DECEMBER")){
//		int iyear=Integer.parseInt(year)+1;
//		bhavName = "bhav" + iyear;
//	}
	
	String corpusQuery="select endingcorpus from portfoliotracker where AsOf=?";
	String insertLiquid="insert into portfolio"+month+year+" values (?,?,1,?,0,0,0)";
	String portfoliocountquery="select count(*) as tickers from portfolio"+month+year;
	
	double allocated=0;

	String query="update portfolio"+month+year+" a inner join "+bhavName+" b on a.symbol=b.symbol and b.mktdate=? set buyprice=b.close,buyqty=round(floor(?/b.close))";
	PreparedStatement selQuery;
	
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

	java.util.Date date = format.parse(closingdate);
//	java.util.Date runningdate = format.parse(runningmonth);

	Calendar cal = Calendar.getInstance();
	cal.setTime(date);
	cal.add(Calendar.MONTH, -1);
	cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));

	String lastday = getLastDayOfTheMonth(format.format(cal.getTime()));
	
//	Calendar calrunning = Calendar.getInstance();
//	cal.setTime(runningdate);
//	String lastrunningday = getLastDayOfTheMonth(format.format(cal.getTime()));


	// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
	// on "+ticker.getMktdate());
	try {
		double allocatePiece=0;
		double unallocated=0;
		double allocatedCapital=0;
		selQuery = connection.prepareStatement(portfoliocountquery);
		ResultSet rs=selQuery.executeQuery();
		int count=0;
		while(rs.next()){
		 count=rs.getInt("tickers");
		}
		
		//get corpus
		
		if(firstRun){
			totalCapital=totalCapital;
			firstRun=false;
		}else{
		
		selQuery = connection.prepareStatement(corpusQuery);
		selQuery.setString(1, closingdate);
		 rs=selQuery.executeQuery();
		while(rs.next()){
			
			totalCapital=rs.getDouble("endingcorpus");
			
		}
		
		}
		allocatedCapital=totalCapital;
		if(momentumdelta<=-35){
			System.err.println("Reduing allocation by 90%");
			allocatedCapital=totalCapital*0.1;
		}else if(momentumdelta<=-20){
			System.err.println("Reduing allocation by 70%");

			allocatedCapital=totalCapital*0.3;
		}
		 allocatePiece=allocatedCapital/countOfStocks;
		 if(count<countOfStocks){
			 unallocated=totalCapital-allocatePiece*count;
		 }
		 if(allocatedCapital<totalCapital){
			 unallocated=totalCapital-allocatePiece*count;

		 }
		selQuery = connection.prepareStatement(query);
		selQuery.setString(1, closingdate);
		selQuery.setDouble(2, allocatePiece);
		
		int update=selQuery.executeUpdate();
		
		//get allocated total amount
		

String totalquery="select sum(Invested) as allocated from (select *,buyprice*buyqty as Invested from portfolio"+month+year+")t";

PreparedStatement prepStat=connection.prepareStatement(totalquery);

rs=prepStat.executeQuery();
double totalallocated=0;
while(rs.next()){
	totalallocated=rs.getDouble("allocated");
}

		
		//insert unallocated entry
		selQuery = connection.prepareStatement(insertLiquid);

		selQuery.setString(1,"Liquid");
		selQuery.setDouble(2,totalCapital-totalallocated);
		selQuery.setDouble(3,totalCapital-totalallocated);

		 update=selQuery.executeUpdate();

	}catch (SQLException e) {
		e.printStackTrace();
		throw e;
	}
}
/**
 * 
 drop table if exists volfilter;
 
 create temporary table volfilter as
select r.symbol,round(avg(r.volume)*close) as vol from tmpmomo t,myrollingperiod r where t.symbol=r.symbol group by r.symbol;
 */
private void eliminateLowVolume(){
	String sql="drop table if exists volfilter create temporary table volfilter as select r.symbol,round(avg(r.volume)*close) as vol from tmpmomo t,myrollingperiod r where t.symbol=r.symbol group by r.symbol;";
}

private void updateWithPositiveDaysCount(String tableName,String startdate,String enddate,String oldbhav,String newbhav)throws SQLException{
	
	String sql="drop table if exists myrollingperiod ;create table myrollingperiod "+
	"select a.closeindictor,a.symbol,a.mktdate,a.volume,a.close from "+oldbhav +" a,tmpmomo b where a.symbol=b.symbol and  a.mktdate>=? and a.mktdate<=?";
	
	if(!oldbhav.equalsIgnoreCase(newbhav)){
		 sql="drop table if exists myrollingperiod ;create table myrollingperiod "+
				"select a.closeindictor,a.symbol,a.mktdate,a.volume,a.close from "+oldbhav +" a,tmpmomo b where a.symbol=b.symbol and  a.mktdate>=? "
				+ "UNION ALL "
				+ "select b.closeindictor,b.symbol,b.mktdate,b.volume,b.close from "+newbhav+" b,tmpmomo c where b.symbol=c.symbol and b.mktdate>=MAKEDATE(year(?),1) and b.mktdate<=?";

	}
	
	
	String sql2="update tmpmomo d ,(select count(closeindictor) as positivedays,symbol,round(avg(volume)*close) as turnover from myrollingperiod  "
			+ "a group by symbol,closeindictor having closeindictor='P')t set d.positivedays=t.positivedays,d.avgvol=t.turnover where d.symbol=t.symbol";
	
//	String sql2="update "+tableName+" d ,(select count(a.closeindictor) as positivedays,a.symbol from "+newbhav +" a,"+tableName +" b "
//			+ "where a.symbol=b.symbol and a.mktdate>=? and a.mktdate<=? group by a.symbol,a.closeindictor having a.closeindictor='P')t set d.positivedays=d.positivedays+t.positivedays where d.symbol=t.symbol";
//	
	
	PreparedStatement selQuery;
	
	

	
	// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
	// on "+ticker.getMktdate());
	try {
		//if(month.equalsIgnoreCase("JANUARY") || month.equalsIgnoreCase("FEB"))
		
		selQuery = connection.prepareStatement(sql);

		selQuery.setString(1, startdate);
		selQuery.setString(2, enddate);

		
		selQuery = connection.prepareStatement(sql);
		
		//appl filter on volume
		
		
		
		
		
		selQuery.setString(1, startdate);
		selQuery.setString(2, enddate);

		
		if(!oldbhav.equalsIgnoreCase(newbhav)){
		selQuery.setString(2, enddate);
		selQuery.setString(3, enddate);
		}
		selQuery.executeUpdate();
		
		selQuery = connection.prepareStatement(sql2);

		
		
		selQuery.executeUpdate();
		
String volquery="delete from tmpmomo where symbol in (select symbol from (select round(avg(volume)*close) as avgvol,symbol from myrollingperiod group by symbol having avgvol<500000)d)";

		
		PreparedStatement pQuery = connection.prepareStatement(volquery);
		pQuery.execute();
		
	}catch(SQLException e){
		e.printStackTrace();
		throw e;
	}
	

}
	// select symbol,BuyPrice,eliminated from portfolio.november2019portfolio
	// where eliminated='N';

	private void loadMomentumBuys(String startdate, String enddate,String runningmonth) throws SQLException,Exception {
		

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		java.util.Date date = format.parse(runningmonth);
		java.util.Date sdate = format.parse(startdate);
		java.util.Date edate = format.parse(enddate);

		  SimpleDateFormat formatte1r=new SimpleDateFormat("MMMM");

		String year = df.format(sdate);
		String runyear = df.format(date);

		String newyear=df.format(edate);
		
		String month = formatte1r.format(date);

		Calendar newcal = Calendar.getInstance();

		newcal.setTime(date);
		//newcal.add(Calendar.MONTH, -1);
		newcal.set(Calendar.DAY_OF_MONTH, newcal.getActualMinimum(Calendar.DAY_OF_MONTH));

	    String currentmonth=format.format(newcal.getTime());
	    String AsOfDay=getLastDayOfTheMonth(currentmonth);

	    String closingday=getLastDayOfThePreviousMonth(currentmonth);

		java.util.Date closingdate = format.parse(closingday);
		String closingyear=df.format(closingdate);

		
		String bhavName = "bhav" + year;
		
		String newbhavname="bhav" + newyear;
		
		String thirdbhav="bhav" + closingyear;

		
//		if(month.equalsIgnoreCase("January") || month.equalsIgnoreCase("February") || month.equalsIgnoreCase("March")){
//			int iyear=Integer.parseInt(year);
//			iyear=iyear-1;
//			 bhavName = "bhav" + iyear;
//			// newbhavname=bhavName;
//
//		}
		// select a.symbol,b.close as startprice,a.close as
		// buyprice,((a.CLOSE-b.CLOSE)/a.CLOSE)*100 AS ROC from bhav2019
		// a,ttcandidates b
		// where b.mktdate='2019-06-03' and a.mktdate='2019-06-28' and
		// a.symbol=b.symbol and a.close>10 and a.close>a.50dma and
		// a.close>a.20dma and a.volume>100000 order by roc desc limit 15;
		
		String  momoTableName="portfolio"+month+runyear;
		
				String firstday = getFirstDayOfTheMonth(runningmonth);
				avgLookbackroc=getMomentumForLookBackPeriod(bhavName, newbhavname, startdate, enddate);
				 avgcurrentroc=getMomentumForCurrentPeriod(bhavName, newbhavname, startdate, closingday);
				 momentumdelta=(avgcurrentroc-avgLookbackroc)/avgLookbackroc*100;


//		String queryCreate = "drop table if exists portfolio"+momoTableName+";create table portfolio"+month+runyear+"(symbol varchar(256),buyprice double,buyqty double,soldprice double,roc double,prevroc double) select a.symbol,b.close as buyprice,0 as buyqty,0 as soldprice,0 as roc,((c.CLOSE-a.CLOSE)/a.CLOSE)*100 AS PrevROC from ttcandidates b, "
//				+ bhavName
//				+ " a, "+newbhavname+" c where a.mktdate=? and b.mktdate=? and c.mktdate=? and a.symbol=b.symbol and a.symbol=c.symbol and c.symbol=b.symbol and c.close>10  and c.volume>100000 and c.close>c.50dma having prevRoc>10  order by PrevROC desc limit 15;";
		
//				
//				String queryCreate = "drop table if exists portfolio"+momoTableName+";create table portfolio"+month+runyear+"(symbol varchar(256),buyprice double,buyqty double,soldprice double,roc double,prevroc double) select a.symbol,b.close as buyprice,0 as buyqty,0 as soldprice,0 as roc,((b.CLOSE-a.CLOSE)/a.CLOSE)*100 AS PrevROC from  "
//					+ bhavName
//						+ " a, "+newbhavname+" b where a.mktdate=? and b.mktdate=?  and a.symbol=b.symbol  and b.close>20  and b.volume>250000 and b.close>b.50dma having prevRoc>13  order by PrevROC desc limit "+countOfStocks;
//	
				
				String tempCreate = "drop table if exists tmpmomo; create table tmpmomo(symbol varchar(256),buyprice double,buyqty double,soldprice double,roc double,prevroc double,positivedays double,avgvol double) select a.symbol,b.close as buyprice,0 as buyqty,0 as soldprice,0 as roc,((b.CLOSE-a.CLOSE)/a.CLOSE)*100 AS PrevROC,0,0 as avgvol from  "
						+ bhavName
							+ " a, "+newbhavname+" b, "+thirdbhav+" c where a.mktdate=? and b.mktdate=? and c.mktdate=?  and a.symbol=b.symbol  and a.symbol=c.symbol and b.symbol=c.symbol and b.close>20  and b.volume>25000 and c.close>c.50dma order by PrevROC desc limit 125";
		
				
				String queryCreate = "drop table if exists "+momoTableName+";create table portfolio"+month+runyear+"(symbol varchar(256),buyprice double,buyqty double,soldprice double,roc double,prevroc double,positivedays double) select a.symbol,a.buyprice as buyprice,0 as buyqty,0 as soldprice,0 as roc,a.PrevRoc AS PrevROC,a.positivedays as positivedays from  "
					 +" tmpmomo a, "+thirdbhav+" c where a.positivedays>(select avg(positivedays) from tmpmomo)-10 and c.mktdate=?  and a.symbol=c.symbol and c.close>c.50dma having prevRoc>"+this.averageLookBackRoc+"  order by PrevROC desc limit "+countOfStocks;
	
				
		String query = "select a.symbol,b.close as startprice,a.close as buyprice,((b.CLOSE-a.CLOSE)/a.CLOSE)*100  AS ROC from ttcandidates b, "
				+ bhavName
				+ " a where a.mktdate=? and b.mktdate=? and a.symbol=b.symbol and a.close>20  and a.volume>100000 and a.close>a.50dma order by roc desc limit "+countOfStocks;
		
		//System.err.println("Query " + queryCreate);
		PreparedStatement selQuery;
		PreparedStatement createQuery;
		PreparedStatement tmpQuery;

		double allocated=totalCapital/countOfStocks;
		
		
		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			tmpQuery = connection.prepareStatement(tempCreate);
			


//			selQuery.setString(1, startdate);
//			selQuery.setString(2, enddate);
//			

			tmpQuery.setString(1, startdate);
			tmpQuery.setString(2, enddate);
			tmpQuery.setString(3, closingday);
			
			

			
			
			int i=tmpQuery.executeUpdate();

			
			updateWithPositiveDaysCount(momoTableName,startdate,closingday,bhavName,thirdbhav);


//			selQuery.setString(1, startdate);
//			selQuery.setString(2, enddate);
//			
			createQuery = connection.prepareStatement(queryCreate);

			
			createQuery.setString(1, closingday);

		

			//ResultSet rs = selQuery.executeQuery();
			 i=createQuery.executeUpdate();
//			trades = new HashSet<Ticker>();
//			Ticker ticker = null;
//			while (rs.next()) {
//				String symbol = rs.getString("symbol");
//				double buyPrice = rs.getDouble("BuyPrice");
//				double stoploss = buyPrice - buyPrice * stoplossLevel / 100;
//				
//				
//				double buyQty = allocated/buyPrice;
//
//				ticker = new Ticker();
//				ticker.setSymbol(symbol);
//				ticker.setBuyPrice(buyPrice);
//				ticker.setStopLoss(stoploss);
//				ticker.setBuyQty(buyQty);
//				ticker.setBuyDate(enddate);
//				trades.add(ticker);
//			}
		//	updateWithPositiveDaysCount(String tableName,String startdate,String enddate,String oldbhav,String newbhav);
				System.err.println("Lookback "+avgLookbackroc);
				
				System.err.println("Current ROC "+avgcurrentroc);


			System.err.println("Momentum Delta "+momentumdelta);
			allocatePortfolio(month,runyear,closingday,momentumdelta);
			calculatePerformance(month,runyear,firstday);
			//updatePortfolioTracker(month, enddate);
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}catch(Exception e){
			e.printStackTrace();
		}

	}

	boolean simulateTrade(Ticker ticker, String mktdate) throws SQLException, Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		if (mktdate == null) {
			return false;
		}
		java.util.Date date = format.parse(mktdate);
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		String year = df.format(date);
		String bhavName = "bhav" + year;
		String query = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma,prevclose,closeindictor,volatility from "
				+ bhavName + " where symbol=? and mktdate=? order by mktdate asc";
		// System.err.println("Query "+query);
		PreparedStatement selQuery;
		boolean exited = false;

		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker.getSymbol());
			selQuery.setString(2, mktdate);
			ResultSet rs = selQuery.executeQuery();

			double buyPrice = ticker.getBuyPrice();
			double stoploss = ticker.getStopLoss();
			// System.err.println(ticker+ "buy "+buyPrice+" stoploss
			// "+stoploss);

			boolean exitTomorrow = ticker.isExitTommorow();

			double exitPrice = 0;
			double scalpPrice = 0;
			double scalpStop = 0;

			int helddays = 0;
			while (rs.next()) {
				double dma50 = rs.getDouble("50dma");
				double dma20 = rs.getDouble("20dma");
				double dma200 = rs.getDouble("200dma");
				String closeindictor = rs.getString("closeindictor");

				// ticker=new Ticker();
				helddays = ticker.getHeldDays() + 1;
				ticker.setHeldDays(helddays);
				ticker.setPositiveIndictor(closeindictor);
				ticker.setDma20(dma20);

				double close = rs.getDouble("close");
				double high = rs.getDouble("high");
				double low = rs.getDouble("low");
				double idealBuyprice = 0;

				double volatility = rs.getDouble("volatility");
				if (helddays > 1) {
					double percentrise = 100 * ((close - buyPrice) / buyPrice);

					if (ticker.getHighest() == 0) {
						ticker.setHighest(high);
					}

				}

				// Handle bonus and splits
				double prevclose = rs.getDouble("prevclose");
				double diffFromPrev = 100 * ((close - prevclose) / prevclose);
				if (diffFromPrev < -15) {
					double adjustedPrice = getAdjustedPrice(ticker, mktdate, ticker.getBuyPrice());
					if (adjustedPrice > 0) {

						buyPrice = ticker.getBuyPrice();
					}

				}

				//

				exitPrice = close;
				ticker.setExitPrice(exitPrice);

				double percentdiff = 100 * ((close - buyPrice) / buyPrice);
				//
				if (exitTomorrow) {
					ticker.setExitDate(mktdate);
					exited = true;
					break;
				}

				// if(close<dma50){
				// ticker.setExitTommorow(true);
				// System.err.println(ticker +" Breached 200 !"+mktdate+"
				// "+dma50);
				// break;
				// }

				if (percentdiff > trailstoplossLevel) {

					double trailstoploss = close - close * trailstoplossLevel / 100;

					ticker.setPassThreshold(true);

					if (trailstoploss > stoploss) {
						// System.err.println(ticker);
						trailstoploss = Math.round(trailstoploss * 100.0) / 100.0;
						ticker.setStopLoss(trailstoploss);
						// System.err.println("Old Stop Loss "+stoploss+" New
						// Stop loss "+trailstoploss+ " Close "+close);
					}
					// System.out.println("Close is : "+close+"New Stop Loss is
					// "+stoploss);
				}

				// if(percentdiff>=25){
				// ticker.setExitDate(mktdate);
				// System.err.println("REached threshold exiting");
				// exited=true;
				// break;
				// }

				if (close < ticker.getStopLoss()) {
					System.out.println(ticker + " Exiting stoploss reached " + mktdate + " close " + close
							+ " stoploss " + ticker.getStopLoss());
					ticker.setExitDate(mktdate);
					exited = true;
					// ticker.setExitPrice(ticker.getStopLoss());
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}

				ticker.setExitDate(mktdate);

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		return exited;
	}

	public double getAdjustedPrice(Ticker ticker, String mktdate, double buyprice) throws Exception {
		String bonusquery = "select symbol,ExDate,Ratio,\"Bonus\" as type from bonus where symbol='"
				+ ticker.getSymbol() + "' and ExDate='" + mktdate + "'";
		if (ticker.getSymbol().equals("VSSL")) {
			System.err.println("Adjust for VSSL ");
		}
		System.err.println("Adjust for  " + ticker.getSymbol());
		double adjustedPrice = 0;
		Statement st = connection.createStatement();
		if (ticker.getBuyDate().equals(mktdate)) {
			System.err.println("Buy date is same as Bonus/Split date .no need to adjust" + ticker);
			return 0;
		}
		ResultSet rs = st.executeQuery(bonusquery);
		while (rs.next()) {
			double ratio = rs.getDouble("ratio");

			adjustedPrice = buyprice / ratio;
			ticker.setBuyPrice(adjustedPrice);
			ticker.setBuyQty(ticker.getBuyQty() * ratio);
			ticker.setStopLoss(ticker.getStopLoss() / ratio);

		}
		String splitquery = "select symbol,ExDate,Ratio,\"Bonus\" as type from splits where symbol='"
				+ ticker.getSymbol() + "' and ExDate='" + mktdate + "'";
		// adjustedPrice = 0;
		st = connection.createStatement();
		rs = st.executeQuery(splitquery);
		while (rs.next()) {
			double ratio = rs.getDouble("ratio");
			buyprice = ticker.getBuyPrice();
			adjustedPrice = buyprice / ratio;
			ticker.setBuyPrice(adjustedPrice);
			ticker.setBuyQty(ticker.getBuyQty() * ratio);
			ticker.setStopLoss(ticker.getStopLoss() / ratio);

		}

		return adjustedPrice;
		// union all
		// select symbol,ExDate,Ratio,\"Split\" as type from splits where
		// ExDate='2010-01-01'
	}
	
	
	
	private void init() throws SQLException{
		connection = TradeUtil.connect();
		Statement st = connection.createStatement();
		st.execute("truncate table portfoliotracker");
		
		st.execute("truncate table consolidatedportfolio");
	}
	private double getMomentumForLookBackPeriod(String oldbhav,String newbhav,String startdate,String enddate)throws SQLException,Exception {
		double averageLookRoc=0;
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

		java.util.Date edate = format.parse(enddate);

		SimpleDateFormat df = new SimpleDateFormat("yyyy");

		String year = df.format(edate);
		newbhav="bhav"+year;
		String query = "select avg(t.roc) as lookbackroc from (select a.symbol,b.close as startprice,a.close as buyprice,((b.CLOSE-a.CLOSE)/a.CLOSE)*100  AS ROC from "+oldbhav +" a,"
				+ newbhav
				+ " b where a.mktdate=? and b.mktdate=? and a.symbol=b.symbol and b.close>20  and b.volume>25000 having ROC >1 order by roc desc limit 125)t  ";
		
		PreparedStatement prep = connection.prepareStatement(query);
		prep.setString(1, startdate);
		prep.setString(2, enddate);
		ResultSet rs = prep.executeQuery();
		
		

		
		
		while (rs.next()) {
			averageLookRoc = rs.getDouble(1);
			;
		}
		this.averageLookBackRoc=averageLookRoc;
		return averageLookRoc;
	}
	
private double getMomentumForCurrentPeriod(String oldbhav,String newbhav,String startdate,String enddate)throws SQLException,Exception {
	double averagecurRoc=0;
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

	java.util.Date edate = format.parse(enddate);

	SimpleDateFormat df = new SimpleDateFormat("yyyy");

	String year = df.format(edate);
	newbhav="bhav"+year;
	String query = "select avg(t.roc) as lookbackroc from (select a.symbol,b.close as startprice,a.close as buyprice,((b.CLOSE-a.CLOSE)/a.CLOSE)*100  AS ROC from "+oldbhav +" a,"
			+ newbhav
			+ " b where a.mktdate=? and b.mktdate=? and a.symbol=b.symbol and b.close>20  and b.volume>25000 order by roc desc limit 125)t ";
	PreparedStatement prep = connection.prepareStatement(query);
	prep.setString(1, startdate);
	prep.setString(2, enddate);
	ResultSet rs = prep.executeQuery();
	while (rs.next()) {
		averagecurRoc = rs.getDouble(1);
		;
	}
	return averagecurRoc;
	}
	
	private String getFirstDayOfTheMonth(String month) throws SQLException {
		String sql = "select distinct mktdate from mktdatecalendar where mktdate>=? order by mktdate asc limit 1";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, month);
		String firstday = null;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			firstday = rs.getString(1);
			;
		}
		return firstday;
	}

	private String getLastDayOfThePreviousMonth(String month) throws SQLException {
		String sql = "select distinct mktdate from mktdatecalendar where mktdate<? order by mktdate desc limit 1";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, month);
		String lastday = null;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			lastday = rs.getString(1);
			;
		}
		return lastday;
	}
	
	private String getLastDayOfTheMonth(String month) throws SQLException {
		String sql = "select distinct mktdate from mktdatecalendar where mktdate<=? order by mktdate desc limit 1";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, month);
		String lastday = null;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			lastday = rs.getString(1);
			;
		}
		return lastday;
	}

	private String getNextDay(String today) throws SQLException {
		String sql = "select distinct mktdate from mktdatecalendar where mktdate>? order by mktdate asc limit 1";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, today);
		String nextday = null;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			nextday = rs.getString(1);
			;
		}
		return nextday;
	}
	
	public static void main(String[] args) throws Exception {
		MomentumSimulator obj = new MomentumSimulator();
		obj.init();

		String startdate = "2017-01-01";
		String enddate = "2019-12-31";

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		obj.currentDay = new java.util.Date(formatter.parse(startdate).getTime());

		Calendar cal = Calendar.getInstance();
		Calendar workingcal = Calendar.getInstance();

		cal.setTime(obj.currentDay);
		workingcal.setTime(obj.currentDay);

		java.util.Date monthoprocess;
		for (int i = 0; i < 260; i++) {
			monthoprocess = cal.getTime();
			workingcal.setTime(cal.getTime());
		  	  SimpleDateFormat formatter1=new SimpleDateFormat("yyyy-MM-dd");
		  
		    String runningmonth=formatter.format(monthoprocess);
			
			workingcal.add(Calendar.MONTH, -2);

			workingcal.set(Calendar.DAY_OF_MONTH, workingcal.getActualMaximum(Calendar.DAY_OF_MONTH));

			String lastday = obj.getLastDayOfTheMonth(formatter.format(workingcal.getTime()));
			workingcal.add(Calendar.MONTH, -4);
//			cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
//			String firstday = obj.getFirstDayOfTheMonth(formatter.format(cal.getTime()));
//			cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
//
//			String lastday = obj.getLastDayOfTheMonth(formatter.format(cal.getTime()));
			
			workingcal.set(Calendar.DAY_OF_MONTH, workingcal.getActualMinimum(Calendar.DAY_OF_MONTH));
			String firstday = obj.getFirstDayOfTheMonth(formatter.format(workingcal.getTime()));
			
			
			System.out.println(firstday);
			System.out.println(lastday);
			obj.loadMomentumBuys(firstday, lastday,runningmonth);
			//
			// java.util.Date currentDay=new
			// java.util.Date(formatter.parse(month).getTime());
			SimpleDateFormat formatte1r = new SimpleDateFormat("MMMM");
			SimpleDateFormat formatter2 = new SimpleDateFormat("YYYY");
			// java.util.Date currentMonth=new
			// java.util.Date(formatte1r.parse(month).getTime());

			System.err.println("Processing " + monthoprocess);
			// obj.setUpDates(monthoprocess);

			obj.mktdates = TradeUtil.loadMarketDates(firstday, lastday);
			String month = formatte1r.format(obj.currentDay).toUpperCase();
			String year = formatter2.format(obj.currentDay).toUpperCase();

//			for (int j = 0; j < obj.mktdates.size(); j++) {
//
//				String today = obj.mktdates.get(j);
//				String nextday = obj.getNextDay(today);
//				for (Ticker t : obj.trades) {
//					obj.simulateTrade(t, nextday);
//				}
//			}
			obj.alltrades.addAll(obj.trades);

			//obj.insertTradeHistory();
			System.err.println("--- Date is -- "+cal.getTime());
			cal.add(Calendar.MONTH, 1);
			System.err.println("--- after Date is -- "+cal.getTime());
			//workingcal=cal;
		}
		
	}
	public final static String tradehistory = "INSERT INTO `bhav`.`tradehistory`(`symbol`,`buydate`,`buyqty`,`buyprice`,`allocatedcapital`,`exitprice`,`exitdate`,`stoplosshit`,`stoplossprice`,squatted,holdingperiod,neilwinner,exittomorrow,roc,roc63,prevclose,highest,scalpprice,scalped,scalpprofit)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private void insertTradeHistory() throws SQLException, Exception {
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		st.execute("truncate table tradehistory");

		PreparedStatement prep = connection.prepareStatement(tradehistory);

		for (Ticker trade : alltrades) {
			prep.setString(1, trade.getSymbol());
			prep.setString(2, trade.getBuyDate());
			if(Double.isInfinite(trade.getBuyQty())){
				System.err.println(trade.getSymbol()+" buy qty less than zero "+trade);
			}
			prep.setDouble(3, trade.getBuyQty());
			prep.setDouble(4,trade.getBuyPrice());
			prep.setDouble(5, Math.round(trade.getAllocatedCapital()));
			
			prep.setDouble(6, trade.getExitPrice());
			prep.setString(7, trade.getExitDate());
			prep.setString(8, trade.isStoplossHit() ? "Y" : "N");
			prep.setDouble(9, trade.getStopLoss());
			prep.setString(10, trade.isSquatted() ? "Y" : "N");
			prep.setInt(11, trade.getHeldDays());
			prep.setString(12, trade.isNeilWinner() ? "Y" : "N");
			prep.setString(13, trade.isExitTommorow()? "Y" : "N");
			prep.setDouble(14, trade.getRoc());
			prep.setDouble(15, trade.getRoc63());
			prep.setDouble(16, trade.getPrevclose());
			prep.setDouble(17, trade.getHighest());
			prep.setDouble(18, trade.getScalpPrice());
			prep.setString(19, trade.isScalped()? "Y" : "N");
			prep.setDouble(20, trade.getScalpProfit());

			prep.addBatch();

		}
		prep.executeBatch();
		connection.commit();
	}
}
