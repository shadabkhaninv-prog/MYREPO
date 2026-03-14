		package bhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class TurtleTradeSimulator {
	Connection connection = null;
	private java.util.Date currentDay;
	ArrayList<Ticker> alltrades = new ArrayList<Ticker>();
	ArrayList<Ticker> tickers = new ArrayList<Ticker>();
	HashSet<Ticker> trades = new HashSet<Ticker>();
	ArrayList<Ticker> completedtrades = new ArrayList<Ticker>();
	HashSet<Ticker> neilwinners = new HashSet<Ticker>();

	ArrayList<String> mktdates = null;
	private static final int maxcapacity = 30;
	private static int availableCapacity=20;
	private int tradecounter = 0;
	private int squatters = 0;
	private static final int stoplossLevel = 6;

	public final static String getPrices = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma from bhav2018";
	public final static String getPricesForTicker = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma from bhav2018 where symbol=? and mktdate=? order by mktdate asc";
	public final static String tradehistory = "INSERT INTO `bhav`.`tradehistory`(`symbol`,`buydate`,`buyqty`,`buyprice`,`allocatedcapital`,`exitprice`,`exitdate`,`stoplosshit`,`stoplossprice`,squatted,holdingperiod,neilwinner,exittomorrow,roc,roc63)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static double totalCapital=1000000;

	private boolean firstAllocationDone=false;
	private double initialCapital=1000000;
	private int initialAllocationCounter=0;
	
	private static double investedCapital=0;
	private double averageroc;
	
	private void connect() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection("jdbc:mysql://localhost/bhav?" + "user=root&password=root");

			System.out.println("Connected !");
		} catch (Exception ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			ex.printStackTrace();
			// System.out.println("SQLState: " + ex.getSQLState());
			// System.out.println("VendorError: " + ex.getErrorCode());
		}

	}
//	
//	void findContractions(){
//		double firsthigh=1744.7;
//		doVCP()
//	}
//	void doContractionHigh(String symbol,String startdate,String enddate) throws SQLException{
//		
//	}
//	void doContractLow(String symbol,String startdate,String enddate) throws SQLException{
//		
//	}
	
	void doVCP(String symbol,String startdate,String enddate) throws SQLException{
		PreparedStatement selQuery;
		selQuery = connection.prepareStatement("select close,mktdate from bhav2018  where symbol=? and mktdate>? and mktdate<?");
		double firstLow;
		
		double secondhigh=0;
		String secondhighdate=null;
		
		double finalhigh;
		String finalhighdate=null;
		double firsthigh=0;
		firstLow=firsthigh;
		String lowDate=null;
		selQuery.setString(1, "VMART");
		selQuery.setString(2, "2018-01-10");
		selQuery.setString(3, "2018-03-05");
		ResultSet rs = selQuery.executeQuery();
		while(rs.next()){
			double close=rs.getDouble("close");
			String mktdate=rs.getString("mktdate");
			if(close<firstLow){
				firstLow=close;
				lowDate=mktdate;
			}
		}
		
		//find final high
		finalhigh=firstLow;
		selQuery.setString(1, "VMART");
		selQuery.setString(2, lowDate);
		selQuery.setString(3, "2018-03-05");
		rs = selQuery.executeQuery();
		while(rs.next()){
			double close=rs.getDouble("close");
			String mktdate=rs.getString("mktdate");
			if(close>finalhigh){
				finalhigh=close;
				finalhighdate=mktdate;
			}
		}
		
		//find second high
	 secondhigh=firstLow;
		selQuery.setString(1, "VMART");
		selQuery.setString(2, lowDate);
		selQuery.setString(3, "2018-03-01");
		rs = selQuery.executeQuery();
		while(rs.next()){
			double close=rs.getDouble("close");
			String mktdate=rs.getString("mktdate");
			if(close>secondhigh){
				secondhigh=close;
				secondhighdate=mktdate;
			}else{
				break;
			}
		}
		
		//find final Low
		double secondLow=secondhigh;
		String secondlowdate=null;
		selQuery.setString(1, "VMART");
		selQuery.setString(2, secondhighdate);
		selQuery.setString(3, "2018-03-01");
		rs = selQuery.executeQuery();
		while(rs.next()){
			double close=rs.getDouble("close");
			String mktdate=rs.getString("mktdate");
			if(close<secondLow){
				secondLow=close;
				secondlowdate=mktdate;
			}else{
				break;
			}
		}
		
		System.err.println("First VCP ");
		System.err.println("Highest "+firsthigh+" Lowest "+firstLow+" On "+lowDate);
		
		System.err.println("Second VCP ");
		System.err.println("Lowest "+firstLow+" Second Highest "+secondhigh+" On "+secondhighdate);

		
		System.err.println("Final VCP ");
		System.err.println("Lowest "+secondLow+" Second Low "+secondlowdate+" Highest "+finalhigh+" On "+finalhighdate);

	}
	
	public boolean goAhead(String mktdate) throws SQLException{
		PreparedStatement st;
		String query="select count(*) as alerts from ttbuyalert where mktdate=?";
		
		st=connection.prepareStatement(query);
		st.setString(1, mktdate);;
		
		ResultSet rs=st.executeQuery();
		
		while(rs.next()){
			int count=rs.getInt("alerts");
			if (count<2) {
				return false;
			}
		}
		
		return true;
		
	}
	
	public static final String inTrendCheck="select intrend from trendarchive where mktdate=?);";

	

	
	public static final String occurenceQuery="select count(*) as occurence from ttbuyalert where symbol=? and mktdate<? and mktdate>date_add(?,interval -30 day);";
	

	
	private boolean checkAverageVolumes(String symbol,String mktdate)throws SQLException,Exception{
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//
//		java.util.Date date = format.parse(mktdate);
//		SimpleDateFormat df = new SimpleDateFormat("yyyy");
//		String year = df.format(date);
//		String bhavName = "bhav" + year;		
//		String volumeCheckQuery="select avg(volume) as volume from "+bhavName+" where symbol=? and mktdate<? limit 10;";
//		//System.err.println(volumeCheckQuery);
//		PreparedStatement selQuery;
//		selQuery = connection
//				.prepareStatement(volumeCheckQuery);
//		selQuery.setString(1, symbol);
//		selQuery.setString(2, mktdate);
//		
//		ResultSet rs = selQuery.executeQuery();
//
//
//		double volume=0;
//		while (rs.next()) {
//			volume=rs.getDouble("volume");
//			
//		}
//		if(volume>1500){
//						return true;
//		}else{
//			System.err.println("Not adding Low Average volume for "+symbol+" is "+volume);
//		}
		return true;
	}
	
	private boolean isFirstOccurence(String symbol,String mktdate)throws SQLException{
		PreparedStatement selQuery;
		selQuery = connection
				.prepareStatement(occurenceQuery);
		selQuery.setString(1, symbol);
		selQuery.setString(2, mktdate);
		selQuery.setString(3, mktdate);
		ResultSet rs = selQuery.executeQuery();


		int occurence=0;
		while (rs.next()) {
			occurence=rs.getInt("occurence");
		}
		if(occurence>1){
			return true;
		}
		return true;
	}
	//public static final String averagerocquery="select avg(roc63) as averageroc63,avg(roc) as averageroc from ttbuyalert where mktdate<=? and mktdate>=date_add(?,interval -180 day)";
	public static final String averagerocquery="select lookbackroc from portfolio.corpus where AsOf=DATE_SUB(LAST_DAY(?),INTERVAL DAY(LAST_DAY(?))-1 DAY)";

	private void loadrocaverages(String mktdate)throws SQLException{
		PreparedStatement selQuery;
		selQuery = connection
				.prepareStatement(averagerocquery);
		selQuery.setString(1, mktdate);
		selQuery.setString(2, mktdate);
		ResultSet rs = selQuery.executeQuery();


		
		while (rs.next()) {
			//averageroc63=rs.getDouble("averageroc63");
			averageroc=rs.getDouble("lookbackroc");
			System.err.println(averageroc+" is for :"+mktdate);
		}
//		if(averageroc63==0){
//			System.err.println("Defaulting average roc "+mktdate);
//			averageroc63=14;
//		}
	
	}
	
	//checks trend
	boolean isFairWeather(String mktdate) throws SQLException{
		PreparedStatement st;
		String query="select intrend from trendarchive where mktdate=?";
		
		st=connection.prepareStatement(query);
		st.setString(1, mktdate);;
		
		ResultSet rs=st.executeQuery();
		
		while(rs.next()){
			int intrend=rs.getInt("intrend");
			if (intrend<18) {
				return false;
			}
		}
		
		return true;
	}
	

	
	void update20DayLow(String mktdate,Ticker t) throws SQLException{
		PreparedStatement selQuery;
		
		selQuery = connection
				.prepareStatement("select min(g.close) as 20dlow from (select close from bhav2017 a where symbol=?"
						+ " and a.mktdate<=? order by mktdate desc limit 20 )g");

		
		selQuery.setString(1, t.getSymbol());
		selQuery.setString(2, mktdate);
		
		//selQuery.setDouble(3, 60);
		//selQuery.setDouble(4, 0);
		
		ResultSet rs = selQuery.executeQuery();

		while (rs.next()) {
			double low=rs.getDouble("20dlow");
			double stoploss=t.getStopLoss();
			
			if(low>stoploss){
				t.setStopLoss(low);
			}
			
		}
		
	}
	
	void loadturtleBuyAlerts(String mktdate, int count) throws Exception{
		
		PreparedStatement selQuery;
		int existingtrades = trades.size();
		
		if (existingtrades == maxcapacity) {
			// System.err.println("Fully allocated Portfolio");
			return;

		}
		if (existingtrades == 0) {
			count = maxcapacity;
		} else {
			count = maxcapacity - existingtrades;
		
			// System.err.println("Room to allocate "+count+" to Portfolio");
		}

		try {
//			
//			if(!isFairWeather(mktdate)){
//				System.err.println("Not good weather to step out "+mktdate);
//				return;
//			}
//			
			//loadrocaverages(mktdate);
			
			// selQuery = connection.prepareStatement("select symbol,mktdate
			// from ttbuyalert where ibdpercentile is not null and ibdpercentile
			// >97 and mktdate=? limit "+count);

//			selQuery = connection
//					.prepareStatement("select symbol,mktdate,(roc3-roc1) as prevroc3,roc,roc63 from ttbuyalert where mktdate=? and roc1>=4 and roc>? and roc63>14 and roc15>0  having prevroc3<=3  order by roc desc limit " + count);
//			selQuery = connection
//					.prepareStatement("select symbol,mktdate,roc,roc63 from ttbuyalert where mktdate=? and roc63>15 and roc>?  order by roc desc limit " + count);

			selQuery = connection
					.prepareStatement("select symbol,mktdate,roc,20dlow,truerange from turtlebuyalerts where mktdate=? order by roc desc limit " + count);

			
			selQuery.setString(1, mktdate);
			//selQuery.setDouble(2, 0);
			//selQuery.setDouble(3, 60);
			//selQuery.setDouble(4, 0);
			
			ResultSet rs = selQuery.executeQuery();
			Ticker ticker = null;
			while (rs.next()) {
				ticker = new Ticker();
				ticker.setSymbol(rs.getString("symbol"));
				ticker.setMktdate(rs.getString("mktdate"));
				ticker.setRoc(rs.getDouble("roc"));
				//ticker.setRoc63(rs.getDouble("roc63"));
				
				if(checkAverageVolumes(ticker.getSymbol(),ticker.getMktdate())){
				boolean added = trades.add(ticker);
				if (added){
					tradecounter++;
				}
				}

				

			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		// System.out.println("Tickers "+trades.size());

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

	public double getAdjustedPrice(Ticker ticker, String mktdate, double buyprice) throws Exception {
		String bonusquery = "select symbol,ExDate,Ratio,\"Bonus\" as type from bonus where symbol='"
				+ ticker.getSymbol() + "' and ExDate='" + mktdate + "'";
		if (ticker.getSymbol().equals("VSSL")) {
			System.err.println("Adjust for VSSL ");
		}
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
			ticker.setBuyQty(ticker.getBuyQty()*ratio);
			ticker.setStopLoss(ticker.getStopLoss() / ratio);

		}
		String splitquery = "select symbol,ExDate,Ratio,\"Bonus\" as type from splits where symbol='"
				+ ticker.getSymbol() + "' and ExDate='" + mktdate + "'";
		//adjustedPrice = 0;
		st = connection.createStatement();
		rs = st.executeQuery(splitquery);
		while (rs.next()) {
			double ratio = rs.getDouble("ratio");
			buyprice = ticker.getBuyPrice();
			adjustedPrice = buyprice / ratio;
			ticker.setBuyPrice(adjustedPrice);
			ticker.setBuyQty(ticker.getBuyQty()*ratio);
			ticker.setStopLoss(ticker.getStopLoss() / ratio);

		}

		return adjustedPrice;
		// union all
		// select symbol,ExDate,Ratio,\"Split\" as type from splits where
		// ExDate='2010-01-01'
	}

	boolean simulateTrade(Ticker ticker, String mktdate) throws SQLException, Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		if (mktdate==null){
			return false;
		}
		java.util.Date date = format.parse(mktdate);
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		String year = df.format(date);
		String bhavName = "bhav" + year;
		String query = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma,prevclose,closeindictor from " + bhavName
				+ " where symbol=? and mktdate=? order by mktdate asc";
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
			
			boolean exitTomorrow=ticker.isExitTommorow();

			double exitPrice = 0;

			int helddays = 0;
			while (rs.next()) {
				double dma50 = rs.getDouble("50dma");
				double dma20 = rs.getDouble("20dma");
				double dma200 = rs.getDouble("200dma");
				String closeindictor=rs.getString("closeindictor");
				
				// ticker=new Ticker();
				helddays = ticker.getHeldDays() + 1;
				ticker.setHeldDays(helddays);
				ticker.setPositiveIndictor(closeindictor);
				ticker.setDma20(dma20);

				double close = rs.getDouble("close");
//				double open = rs.getDouble("open");
//				
//				double close=(rclose+open)/2;
//				
				

				if (ticker.getBuyDate() == null) {
					double bodayLow=rs.getDouble("low");
					buyPrice = close;
					ticker.setBuyPrice(buyPrice);
					ticker.setBodayLow(bodayLow);
					ticker.setBuyDate(mktdate);
					stoploss = buyPrice - buyPrice * stoplossLevel / 100;
					
					//stoploss=Math.round(stoploss*100.0)/100.0;
					ticker.setStopLoss(stoploss);
					getAllocation(ticker);
					availableCapacity=availableCapacity-1;

					// System.out.println("BuyPrice : "+buyPrice+" Stoploss
					// "+stoploss);
				}
				
				// Handle bonus and splits
				double prevclose = rs.getDouble("prevclose");
				double diffFromPrev = 100 * ((close - prevclose) / prevclose);
				if (diffFromPrev < -10) {
					double adjustedPrice = getAdjustedPrice(ticker, mktdate, ticker.getBuyPrice());
					if (adjustedPrice > 0) {

						buyPrice = ticker.getBuyPrice();
					}

				}
				
				//
				
				exitPrice = close;
				ticker.setExitPrice(exitPrice);

				double percentdiff = 100 * ((close - buyPrice) / buyPrice);
				
				if(exitTomorrow){
					ticker.setExitDate(mktdate);
					exited = true;
					break;
				}
			
//				 if(close<dma50){
//					 ticker.setExitTommorow(true);
//					 System.err.println(ticker +" Breached 200 !"+mktdate+" "+dma50);
//					 break;
//					 }
				
		
				
				
				
				if (percentdiff > 14) {

					double trailstoploss = close - close * 14 / 100;
				
				
					
//					if (helddays > 30){
//						trailstoploss = close - close * 10 / 100;
//					}
//					
//					if (helddays > 120){
//						trailstoploss = close - close * 12 / 100;
//					}
					
//					if (helddays > 90){
//						trailstoploss = close - close * 10 / 100;
//					}
					
					
					if (trailstoploss > stoploss) {
						//System.err.println(ticker);
						trailstoploss=Math.round(trailstoploss*100.0)/100.0;
						ticker.setStopLoss(trailstoploss);
						// System.err.println("Old Stop Loss "+stoploss+" New
						// Stop loss "+trailstoploss+ " Close "+close);
					}
					// System.out.println("Close is : "+close+"New Stop Loss is
					// "+stoploss);
				}
				
			
				if (close < ticker.getStopLoss()) {
					// System.out.println("Exiting stoploss reached "+mktdate +"
					// close "+close+" stoploss "+ticker.getStopLoss());
					ticker.setExitDate(mktdate);
					exited = true;

					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
//				if(helddays <15 && close<ticker.getDma50()){
//					System.err.println(ticker +" below 50dma");
//				}

//				if ((helddays ==2) && ticker.getPositiveIndictor().equalsIgnoreCase("N")){
//					System.err.println(ticker+"Positive Indictor"+ticker.getPositiveIndictor());
//					ticker.setExitTommorow(true);
////					ticker.setExitDate(mktdate);
////					ticker.setSquatted(true);
////					exited = true;
////					squatters++;
//					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
//					break;
//				}
				
				

//				
				if (helddays == 3 && percentdiff < -5) {
					ticker.setExitTommorow(true);
//					ticker.setExitDate(mktdate);
//					ticker.setSquatted(true);
//					exited = true;
//					squatters++;
//					System.err.println(ticker+" 10 day Falling more than 6 percent "+percentdiff);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
				
				
				// exit positions that have squatted long and made no major
				// profits

//
//				if (helddays > 10 && percentdiff < -6) {
//					ticker.setExitTommorow(true);
////					ticker.setExitDate(mktdate);
////					ticker.setSquatted(true);
////					exited = true;
////					squatters++;
////					System.err.println(ticker+" 10 day Falling more than 6 percent "+percentdiff);
//					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
//					break;
////				}
				if (helddays > 20 && percentdiff < -5) {
					//System.err.println(ticker+" 20 day Falling more than 5 percent "+percentdiff);
					ticker.setExitTommorow(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
				
	
				if (helddays > 30 && percentdiff < 2) {
					//System.err.println(ticker+" 20 day Falling more than 5 percent "+percentdiff);
					ticker.setExitTommorow(true);
					ticker.setSquatted(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
				if (helddays > 35 && percentdiff < 5) {
					ticker.setExitTommorow(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
				
//				if ((helddays > 30 && helddays<41) && percentdiff < 5) {
//					System.err.println(ticker+" 30 day squatted at 5 percent "+percentdiff);
//
//					ticker.setExitDate(mktdate);
//					ticker.setSquatted(true);
//					exited = true;
//					squatters++;
//					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
//					break;
//				}
//				
				if ((helddays > 40) && percentdiff < 10) {
					ticker.setExitTommorow(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
//				
				if ((helddays > 60) && percentdiff < 15) {
					ticker.setExitTommorow(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
				if ((helddays > 75) && percentdiff < 20) {
					ticker.setExitTommorow(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
				
//				if ((helddays > 120) && percentdiff < 30) {
//					ticker.setExitTommorow(true);
//					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
//					break;
//				}

				ticker.setExitDate(mktdate);
			

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		return exited;
	}

	private void loadMarketDates(String startdate, String enddate) throws SQLException {
		String sql = "select distinct mktdate from mktdatecalendar where mktdate>=? and mktdate<=? order by mktdate asc";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, startdate);
		prep.setString(2, enddate);
		ResultSet rs = prep.executeQuery();
		mktdates = new ArrayList<String>();
		while (rs.next()) {
			mktdates.add(rs.getString(1));
		}

	}

	// public final static String tradehistory="INSERT INTO
	// `bhav`.`tradehistory`(`symbol`,`buydate`,`buyprice`,`exitprice`,`exitdate`,`stoplosshit`,`stoplossprice`,`25daysquat`,`60daysquat`,'holdingperiod')VALUES(?,?,?,?,?,?,?,?,?,?)";
	private void insertTradeHistory() throws SQLException, Exception {
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		st.execute("truncate table tradehistory");

		PreparedStatement prep = connection.prepareStatement(tradehistory);

		for (Ticker trade : alltrades) {
			prep.setString(1, trade.getSymbol());
			prep.setString(2, trade.getBuyDate());
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

			prep.addBatch();

		}
		prep.executeBatch();
		connection.commit();
	}
	
	private void insertEODStatus() throws SQLException, Exception {
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		st.execute("truncate table dailycapitalstatus");
		PreparedStatement prep = connection.prepareStatement(eodcapital);

		for (DailyCapitalStatus capital : capitalList) {
			String mktdate=capital.getMktdate();
			if(mktdate!=null){
			prep.setString(1, mktdate);
			prep.setDouble(2, capital.getInvestedCapital());
			prep.setDouble(3, capital.getAvailableCapital());
			prep.addBatch();
			}

		}
		prep.executeBatch();
		connection.commit();
	}
	
	private void getAllocation(Ticker trade){
		if(!firstAllocationDone){
		initialAllocationCounter++;
		}
		double capitalToUse=0;
		double tradeCapital=0;
		if(!firstAllocationDone){
			capitalToUse=initialCapital;
			tradeCapital=capitalToUse/maxcapacity;
			
		}else{
			capitalToUse=totalCapital;
			tradeCapital=capitalToUse/availableCapacity;
		}

		
		
		totalCapital=totalCapital-tradeCapital;
		double boughtQuantity=tradeCapital/trade.getBuyPrice();
		trade.setBuyQty(boughtQuantity);
		trade.setAllocatedCapital(tradeCapital);
		if(initialAllocationCounter==8){
			firstAllocationDone=true;
		}
		
	}
	
	private ArrayList<DailyCapitalStatus> capitalList=new ArrayList<DailyCapitalStatus>();
	public static final String eodcapital="insert into dailycapitalstatus values(?,?,?)";
	
	private void calculateEOD(String mktdate) {
		DailyCapitalStatus obj=new DailyCapitalStatus();
		double capitalinplay=0;
		double originalcost=0;
		for (Ticker trade : this.trades) {
			double invested = trade.getBuyPrice() * trade.getBuyQty();
			double currentvalue = trade.getExitPrice() * trade.getBuyQty();
			//System.err.println(trade+" "+invested);
			capitalinplay = capitalinplay + currentvalue;
			originalcost=originalcost+invested;

		}
		obj.setMktdate(mktdate);
		obj.setInvestedCapital(capitalinplay);
		obj.setAvailableCapital(totalCapital);
		capitalList.add(obj);
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		double winners = 0;
		double losers = 0;
		double totalwin = 0;
		double totalloss = 0;
		//totalCapital=100000;o
		//System.err.println("Capital "+totalCapital);

		TurtleTradeSimulator obj = new TurtleTradeSimulator();
		String startdate = "2017-01-01";
		String enddate = "2017-12-31";
		obj.connect();
		//obj.doVCP();
		obj.loadMarketDates(startdate, enddate);

		int count = maxcapacity;
		
		
		double totalearned = 0;
		int holdingperiod = 0;
		//double allocation=capital/8;
		// obj.currentDay=new
		// java.util.Date(formatter.parse(buydate).getTime());

		System.err.println("Market dates " + obj.mktdates.size());
	

		for (int i = 0; i < obj.mktdates.size(); i++) {
			
			String today = obj.mktdates.get(i);
			String nextday = obj.getNextDay(today);
			obj.loadturtleBuyAlerts(today, count);
			// System.err.println("trading on "+today);
			Iterator<Ticker> iterator = obj.trades.iterator();
			while (iterator.hasNext()) {
				Ticker ticker = iterator.next();

				boolean exited = obj.simulateTrade(ticker, nextday);
				if (exited) {
					
						double tradeProceeds=ticker.getExitPrice()*ticker.getBuyQty();
						//System.err.println("Capital "+totalCapital);

						totalCapital=totalCapital+tradeProceeds;
						//System.err.println(ticker+" exited "+tradeProceeds+" Capital "+totalCapital);
					
					obj.completedtrades.add(ticker);
					// System.err.println(ticker.getSymbol()+" exited
					// "+ticker.getExitDate());
					iterator.remove();
					obj.availableCapacity++;

				}

			}
			
			obj.calculateEOD(nextday);

		}


		obj.alltrades.addAll(obj.completedtrades);
		obj.alltrades.addAll(obj.trades);
		System.err.println("Completed Trades " + obj.completedtrades.size());
		System.err.println("Open Trades " + obj.trades.size());
		System.err.println("AvailableCapacity "+availableCapacity);
		System.err.println("Available Capital "+totalCapital);
		
		
		
		for (Ticker trade : obj.trades) {
			double invested=trade.getExitPrice()*trade.getBuyQty();
			investedCapital=investedCapital+invested;
			
		}
		System.err.println("Invested Capital "+investedCapital);
		
		System.err.format("%f%n",investedCapital);
		for (Ticker trade : obj.alltrades) {
			holdingperiod = holdingperiod + trade.getHeldDays();
			double exitprice = trade.getExitPrice();
			double buyprice = trade.getBuyPrice();
			double Bought = trade.getBuyQty();
			double investedamount=Bought*buyprice;
			double Sold = exitprice * Bought;
			double gainloss = Sold - (Bought * buyprice);
			gainloss = Math.round(gainloss * 100.0) / 100.0;
			double Result = investedamount + gainloss;

			if (exitprice >= buyprice) {
				winners++;
				double gain = 100 * ((exitprice - buyprice) / buyprice);
				double profit = exitprice - buyprice;

				totalwin = totalwin + gain;
//				System.out.println(trade.getSymbol() + " Bought on " + trade.getBuyPrice() + " on " + trade.getBuyDate()
//						+ "  Exit Price " + trade.getExitPrice() + " on " + trade.getExitDate() + " Gain :" + gainloss
//						+ " Held :" + trade.getHeldDays() + " Days" + " Invested:" + investedamount + " Stop loss: "
//						+ trade.getStopLoss());
			} else {
				losers++;
				double loss = 100 * ((exitprice - buyprice) / buyprice);
				double lost = buyprice - exitprice;
				// capital=capital-lost;
//				System.err.println(trade.getSymbol() + " Bought on " + trade.getBuyPrice() + " on " + trade.getBuyDate()
//						+ "  Exit Price " + trade.getExitPrice() + " on " + trade.getExitDate() + " Lost :" + gainloss
//						+ " Held :" + trade.getHeldDays() + " Days" + " Stop loss: " + trade.getStopLoss());
//				if (loss < -10) {
//					System.err.println(trade + " Lost " + loss);
//				}
				totalloss = totalloss + loss;

			}
			totalearned = totalearned + Result;
		}
		obj.insertTradeHistory();
		obj.insertEODStatus();
		double totaltrades=winners+losers;
		
		double perwin=winners / totaltrades;
		perwin *=100;
		
		System.err.println("Winners " + winners + " Losers " + losers+" total "+totaltrades);
		System.err.println("Win % "+perwin +" Loss "+(losers/totaltrades)*100);
		//System.err.println("Avg Holding period :" + holdingperiod / obj.completedtrades.size() + " Days");

		System.err.println("Win % " + totalwin + " Lost% " + totalloss);
		System.err.println("Average Win % " + totalwin / winners + " Lost% " + totalloss / losers);

		
		System.err.println("Total trades added :" + obj.tradecounter);
		System.err.println("Completed trades  :" + obj.completedtrades.size());
		System.err.println("Open trades  :" + obj.trades.size());
		System.err.println("Squatters  :" + obj.squatters);
		System.err.println("Open Trade  :" + obj.trades);
////
////		System.err.println("Neil winners  :" + obj.neilwinners.size());
////		for (Ticker trade : obj.neilwinners) {
////			System.err.println(
////					trade + " " + trade.getBuyPrice() + " " + trade.getExitPrice() + " Gain " + trade.getPercentGain());
////		}

	}

}
