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

public class VolumeSurgeSimulator {
	Connection connection = null;
	private java.util.Date currentDay;
	ArrayList<Ticker> alltrades = new ArrayList<Ticker>();
	ArrayList<Ticker> tickers = new ArrayList<Ticker>();
	HashSet<Ticker> trades = new HashSet<Ticker>();
	ArrayList<Ticker> completedtrades = new ArrayList<Ticker>();
	HashSet<Ticker> neilwinners = new HashSet<Ticker>();
	
	
	
	ArrayList<String> mktdates = null;

	private int tradecounter = 0;
	private int squatters = 0;
	
	static final int TOTALHOLDINGS=6;
	
	private static final double stoplossLevel =3;
	private static final double surgeFactor=1.5;

	private static final double trailstoplossLevel =3;
	
	private static final int scalplevel =4;


	private static final int maxcapacity = TOTALHOLDINGS;
	private static int availableCapacity=TOTALHOLDINGS;
	public final static String tradehistory = "INSERT INTO `bhav`.`cantradehistory`(`symbol`,`buydate`,`buyqty`,`buyprice`,`allocatedcapital`,`exitprice`,`exitdate`,`stoplosshit`,`stoplossprice`,squatted,holdingperiod,neilwinner,exittomorrow,roc,roc63,prevclose,highest,scalpprice,scalped,scalpprofit,lastvolume,lasthigh,surgefactor,crossedhigh)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static double totalCapital=500000;

	private boolean firstAllocationDone=false;
	private double initialCapital=500000;
	private int initialAllocationCounter=0;
	
	private static double investedCapital=0;
	private static double scalpedCapital=0;
	private double averageroc;
	private int scalpCount=0;
	private double gapperCount=0;
	private double pivotPlus=0.3;
	private double samedaystoplosslevel=3;
	
	private int checkVolumePercentile(Ticker ticker,String newbhav,String oldbhav,String mktdate,double volume) throws Exception{
		String volumePercentilQuery="select count(*) as rank from(select  symbol,mktdate,volume  from "
				+ oldbhav+"  WHERE mktdate>=?-interval 1 year and symbol=? and volume> ? UNION ALL select  symbol,mktdate,volume  from "
						+ newbhav+"  WHERE mktdate<=? and symbol=? and volume> ?) t";
		PreparedStatement selQuery;
		ResultSet rs=null;
		int rank=0;
		try{
		 selQuery = connection.prepareStatement(volumePercentilQuery);
		 selQuery.setString(1, mktdate);
		 selQuery.setString(2, ticker.getSymbol());
		 selQuery.setDouble(3, volume);
		 selQuery.setString(4, mktdate);

		 selQuery.setString(5, ticker.getSymbol());

		 selQuery.setDouble(6, volume);
		 rs=selQuery.executeQuery();
		 while (rs.next()) {
				 rank = rs.getInt("rank");
				System.err.println("Rank is "+ticker+" -- "+rank);
		 }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			rs.close();
			rs=null;
		}
		 return rank;
	}

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
	

	

		
	//public static final String averagerocquery="select avg(roc63) as averageroc63,avg(roc) as averageroc from ttbuyalert where mktdate<=? and mktdate>=date_add(?,interval -180 day)";
	public static final String averagerocquery="select lookbackroc from portfolio.corpus where AsOf=DATE_SUB(LAST_DAY(?),INTERVAL DAY(LAST_DAY(?))-1 DAY)";

	//checks trend

	
	void loadCandidates(String mktdate) throws Exception{
		PreparedStatement selQuery;
		ResultSet rs=null;
		int existingtrades = trades.size();
		
		if (existingtrades == maxcapacity) {
			// System.err.println("Fully allocated Portfolio");
			return;

		}
	

		try {
			//System.err.println("Loading candidates for "+mktdate);
			StringBuilder sbSql = new StringBuilder( 1024 );
			sbSql.append( "select symbol,volume,avgvol,highest,distancefromhigh,dayssincehigh,(avgvol*close) as turnover from ttcandidates where symbol<>'BRIGADE' and symbol<>'SUNCLAYLTD' and  mktdate=? and symbol not in(" );
			
			String[] Parameter;
			Parameter=new String[trades.size()];
			if(!trades.isEmpty()){
				int p=0;
				for(Ticker t:trades){
				
					//System.err.println("Loading parameters .."+p);
		
		//			System.err.println(t.getSymbol());
					Parameter[p]=t.getSymbol();
					p++;
			}
			//System.err.println("Parameter "+Parameter.toString());
			for( int i=0; i < Parameter.length; i++ ) {
			  if( i > 0 ) sbSql.append( "," );
			  sbSql.append( " ?" );
			}
			}// for
			else{
				sbSql.append( " ?" );
			}
			sbSql.append( " )  and close>10 and symbol not REGEXP 'NIFTY|NSE|ETF|BEES|GOLD|RELBANK' having turnover>"+Constants.turnoverCutoff+" order by distancefromhigh desc limit 100");
			//System.err.println("Query is "+sbSql.toString());
			 selQuery = connection.prepareStatement( sbSql.toString() );
			
			selQuery.setString(1, mktdate);
			//selQuery.setDouble(2, 0);
			if(trades.isEmpty()){
				selQuery.setString(2,"ignore");
			}
			for( int i=0; i < Parameter.length; i++ ) {
				selQuery.setString( i+2, Parameter[ i ] );
			} // for
			

			rs = selQuery.executeQuery();
			Ticker ticker = null;
			while (rs.next()) {
				int nooftrades = trades.size();
				
				if (nooftrades == maxcapacity) {
					// System.err.println("Fully allocated Portfolio");
					return;

				}
				
				ticker = new Ticker();
				ticker.setSymbol(rs.getString("symbol"));
				//ticker.setMktdate(rs.getString("mktdate"));
				ticker.setPivotBuyPrice(rs.getDouble("highest"));
				ticker.setBreakoutVolume(rs.getDouble("avgvol"));
				ticker.setLastVolume(rs.getDouble("volume"));
				String tradingday=getNextDay(mktdate);
				
				
				boolean breakout=breakoutSimulation(tradingday, ticker);
				if(breakout){
					
			//	System.err.println("Taking trade post breakout");
				boolean added = trades.add(ticker);
				if (added){
					tradecounter++;
					//System.err.println("Taking trade post breakout "+ticker.getSymbol()+" on "+tradingday);
				}
				}

				}

			

		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			rs.close();
			rs=null;
		}
		// System.out.println("Tickers "+trades.size());

	}
	
	boolean breakoutSimulation(String mktdate,Ticker ticker) throws SQLException,Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		double cutoffVolume=ticker.getBreakoutVolume()*surgeFactor;		
				
		double expectedVolume=ticker.getLastVolume()*surgeFactor-1;		
		if (mktdate==null){
			return false;
		}
		//System.err.println("PRocessing "+mktdate);
		java.util.Date date = format.parse(mktdate);																																					
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		String year = df.format(date);
		String bhavName = "bhav" + year;
		String query = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma,prevclose,closeindictor,volatility,volume,round(((close-prevclose)/prevclose)*100,2) as diff from " + bhavName
				+ " where symbol=? and mktdate=? order by mktdate asc";
		int oldyear=Integer.parseInt(year)-1;
		String oldbhav="bhav"+ oldyear;
		PreparedStatement selQuery;
		ResultSet rs=null;
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker.getSymbol());
			selQuery.setString(2, mktdate);
			 rs = selQuery.executeQuery();
			
			while(rs.next()){

				double close = rs.getDouble("close");
				double open = rs.getDouble("open");
				double prevclose=rs.getDouble("prevclose");
				double high = rs.getDouble("high");
				double low = rs.getDouble("low");
				double volume=rs.getDouble("volume");
				double volatility=rs.getDouble("volatility");
				double prevclosediff=rs.getDouble("diff");
				//if(checkAverageVolumes(ticker.getSymbol(),mktdate)){
				double idealBuyprice =ticker.getPivotBuyPrice()+ ticker.getPivotBuyPrice()*pivotPlus/100;
				double surge=volume/ticker.getBreakoutVolume();
				
				double difffrompivot=100 * ((idealBuyprice - close) /close);
				int rank=0;

				ticker.setSurgeFactor(surge);
				if(volume>cutoffVolume && (prevclosediff>=0.5 && prevclosediff<=3 && difffrompivot<9 )){
					//System.err.println(ticker+" Diff from pivot is "+difffrompivot+" high "+idealBuyprice);

					//System.out.println(ticker.getSymbol() +" Has broken out above "+ticker.getPivotBuyPrice()+" High is "+high+" On volume "+cutoffVolume);
//					 rank=checkVolumePercentile(ticker,bhavName,oldbhav,mktdate,volume);
//					 ticker.setVolumerank(rank);
//					if(rank<=15)	return true; else return false;
					String nextday=getNextDay(mktdate);
					double nextdayclose=getNextDayClose(nextday, bhavName,ticker);
					double difffromprevday=100 * ((nextdayclose - close) /close);
					if(difffromprevday<1){
						System.err.println(ticker+" Diff from prev day is "+difffromprevday);
					return true;
					}
				}
			
			//}
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}finally{
			rs.close();
		}
		return false;
	}
	//select close from bhav2019 where mktdate='2019-12-03' and symbol='GLAXO'

	private double getNextDayClose(String nextday,String bhav,Ticker ticker)throws SQLException{
		String sql = "select close from "+bhav+ " where mktdate=? and symbol=?";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, nextday);
		prep.setString(2, ticker.getSymbol());

		double nextclose = 0;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			nextclose = rs.getDouble(1);
			;
		}
		return nextclose;
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
	
	private double getMaxVolume(String ticker,String mktdate,String bhav){
		double maxvolume=0;
		String query="select max(volume) as maxvolume from "+ bhav +" where symbol=? and  mktdate<?";
		PreparedStatement selQuery;
		boolean exited = false;
		

		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker);
			selQuery.setString(2, mktdate);
			//selQuery.setString(3, mktdate);
			ResultSet rs = selQuery.executeQuery();
			while (rs.next()) {
				maxvolume = rs.getDouble("maxvolume");

			}
			System.err.println("MAx Volume is "+maxvolume+" for "+ticker+" from mktdate "+mktdate);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return maxvolume;

	}
	
	private double getAverageVolatility(String ticker,String mktdate,String bhav){
		double avgvolatility=0;
		String query="select avg(volatility) as avgvolatility from "+ bhav +" where symbol=? and  mktdate<? and mktdate>?-interval 30 DAY";
		PreparedStatement selQuery;
		boolean exited = false;
		

		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker);
			selQuery.setString(2, mktdate);
			selQuery.setString(3, mktdate);
			ResultSet rs = selQuery.executeQuery();
			while (rs.next()) {
				 avgvolatility = rs.getDouble("avgvolatility");

			}
			//System.err.println("Avg Volatility is "+avgvolatility+" for "+ticker+" from mktdate "+mktdate);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return avgvolatility;

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
		String query = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma,prevclose,closeindictor,volatility from " + bhavName
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


			double exitPrice = 0;
			double scalpPrice=0;
			int count=0;
			int helddays = 0;
			while (rs.next()) {
				count++;
				String closeindictor=rs.getString("closeindictor");
				
				// ticker=new Ticker();
				helddays = ticker.getHeldDays() + 1;
				ticker.setHeldDays(helddays);
				ticker.setPositiveIndictor(closeindictor);
				

				double close = rs.getDouble("close");
				double open = rs.getDouble("open");

				double high = rs.getDouble("high");
				double low = rs.getDouble("low");
				double idealBuyprice=0;
				
				if (ticker.getBuyDate() == null) {
					double bodayLow=rs.getDouble("low");
					double prevclose=rs.getDouble("prevclose");
					ticker.setPrevclose(prevclose);
					double bodayhigh=rs.getDouble("high");
					String nextday=getNextDay(mktdate);
					double nextdayclose=getNextDayClose(nextday, bhavName,ticker);
					//buyPrice = close;

					idealBuyprice =ticker.getPivotBuyPrice()+ ticker.getPivotBuyPrice()*pivotPlus/100;
					buyPrice=nextdayclose;;
					if(ticker.isGappedup()){
						System.err.println(ticker.getSymbol()+" Gapped up "+ticker.getBuyPrice());
						buyPrice=ticker.getBuyPrice();
					}
				
					//ticker.setBuyPrice(idealBuyprice);
					ticker.setBuyPrice(buyPrice);
					ticker.setBodayLow(bodayLow);
					ticker.setBreakoutHigh(bodayhigh);
					
					ticker.setBuyDate(mktdate);
					
					//System.err.println(ticker.getSymbol()+" Buy price "+idealBuyprice+" Close "+buyPrice+" On "+ticker.getBuyDate());
				//	System.err.println(ticker.getSymbol()+" IDeal Buy price "+idealBuyprice+" Pivot price "+ticker.getPivotBuyPrice()+" On "+ticker.getBuyDate());
					
//					if(ticker.getSurgeFactor()<=3){
//						stoploss = buyPrice - buyPrice * 2.5 / 100;
//					}else{
//					stoploss=bodayLow;
//					}
					//stoploss=bodayLow;
					stoploss = buyPrice - buyPrice * stoplossLevel / 100;

//					if(difftolow>5){
//						System.err.println("Low too far "+ticker);
//						stoploss = buyPrice - buyPrice * 4 / 100;
//					}
					//stoploss = buyPrice - buyPrice * 4 / 100;
					//stoploss=Math.floor(stoploss);
					scalpPrice=buyPrice + buyPrice * scalplevel / 100;
				//	System.err.println("First time "+stoploss);
					ticker.setStopLoss(stoploss);
					ticker.setScalpPrice(scalpPrice);
					//ticker.setScalpStoploss(scalpStop);
					getAllocation(ticker);
					availableCapacity=availableCapacity-1;
					
					
				}
//				

		
				System.err.println(mktdate+" Stop loss "+ticker+" is "+ticker.getStopLoss());


				
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

				
				double trailstoploss = close - close * trailstoplossLevel / 100;
				//double trailstoploss = ticker.getStopLoss();
				
				ticker.setPassThreshold(true);

				
				
				if (percentdiff >trailstoplossLevel) {

					 trailstoploss = close - close * trailstoplossLevel / 100;
				
					
					ticker.setPassThreshold(true);

					
					
					if (trailstoploss > stoploss) {
						//System.err.println(ticker);
						trailstoploss=Math.floor(trailstoploss*100.0)/100.0;
						ticker.setStopLoss(trailstoploss);
						// System.err.println("Old Stop Loss "+stoploss+" New
						// Stop loss "+trailstoploss+ " Close "+close);
					}
					// System.out.println("Close is : "+close+"New Stop Loss is
					// "+stoploss);
				}
			
				
//				
//				if (helddays>1 && open < ticker.getStopLoss()) {
//					System.err.println(ticker+" Stoploss is "+ticker.getStopLoss());
//					ticker.setExitDate(mktdate);
//					exited = true;
//					double soldPrice=open- open*0.2/100;
//					ticker.setExitPrice(soldPrice);
//					ticker.setStoplossHit(true);
//
//					// trades.add(ticker);
//					break;
//
//				}
				
				if(helddays>1 && high>ticker.getPivotBuyPrice()){
					System.err.println(ticker +" Crossed high ");
					ticker.setCrossedHigh(true);
//					double soldPrice=high;
//					ticker.setExitPrice(soldPrice);
//					ticker.setStoplossHit(true);
//
//					// trades.add(ticker);
//					break;
				}
				
				if (helddays>1 && close < ticker.getStopLoss()) {
					System.err.println(ticker+" Stoploss is "+ticker.getStopLoss());
					ticker.setExitDate(mktdate);
					exited = true;
					double soldPrice=close;

					ticker.setExitPrice(soldPrice);
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
				
				if (helddays > 30 && percentdiff < 1	) {
					//System.err.println(ticker+" 20 day Falling more than 5 percent "+percentdiff);
					ticker.setExitTommorow(true);
					ticker.setSquatted(true);
					System.err.println("Cutting loser " + ticker.getSymbol() + " Squatted!");
					break;
				}
				
//				if (helddays>1 && high < ticker.getBreakoutHigh()) {
//					System.err.println(ticker+" Did not breach breakout high "+ticker.getBreakoutHigh());
//					ticker.setExitDate(mktdate);
//					trailstoploss = buyPrice - buyPrice * 2 / 100;
//					
//					trailstoploss=Math.floor(trailstoploss*100.0)/100.0;
//					if(ticker.getStopLoss()<trailstoploss){
//					ticker.setStopLoss(trailstoploss);
//					}
//
//					// trades.add(ticker);
//					break;
//
//				}
				

				
		
				

//			
			//	System.err.println("Close "+close+" "+ mktdate);

			

				


				ticker.setExitDate(mktdate);
			

			}
			
			if(count==0){
				System.err.println(ticker+" is empty exiting "+ticker.getExitDate());
				exited = true;

				ticker.setStoplossHit(true);

				// trades.add(ticker);
			
				
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
		st.execute("truncate table cantradehistory");

		PreparedStatement prep = connection.prepareStatement(tradehistory);

		for (Ticker trade : alltrades) {
			prep.setString(1, trade.getSymbol());
			prep.setString(2, trade.getBuyDate());
			//System.err.println(trade);
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
			prep.setDouble(21, trade.getBreakoutVolume());
			prep.setDouble(22, trade.getPivotBuyPrice());
			prep.setDouble(23, trade.getSurgeFactor());
			prep.setString(24, trade.isCrossedHigh() ? "Y" : "N");

			prep.addBatch();

		}
		prep.executeBatch();
		connection.commit();
	}

	private void insertEODStatus() throws SQLException, Exception {
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		st.execute("truncate table cancapitalstatus");
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
		if(initialAllocationCounter==TOTALHOLDINGS){
			firstAllocationDone=true;
		}
		
	}
	
	private ArrayList<DailyCapitalStatus> capitalList=new ArrayList<DailyCapitalStatus>();
	public static final String eodcapital="insert into cancapitalstatus values(?,?,?)";
	
	private void calculateEOD(String mktdate) {
		DailyCapitalStatus obj=new DailyCapitalStatus();
		double capitalinplay=0;
		double originalcost=0;
		for (Ticker trade : this.trades) {
			double invested = trade.getBuyPrice() * trade.getBuyQty();
			double currentvalue = trade.getExitPrice() * trade.getBuyQty();
			//System.err.println(trade+" "+invested);
			//System.err.println(trade+" "+currentvalue);

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
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		String date = simpleDateFormat.format(new java.util.Date());
		System.out.println(date);

		VolumeSurgeSimulator obj = new VolumeSurgeSimulator();
		String startdate = "2014-01-01";
		String enddate = "2014-12-31";
		//enddate = date;
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
			//obj.loadBuyAlerts(today, count);
			obj.loadCandidates(today);
			
			// System.err.println("trading on "+today);
			Iterator<Ticker> iterator = obj.trades.iterator();
			while (iterator.hasNext()) {
				Ticker ticker = iterator.next();

				boolean exited = obj.simulateTrade(ticker, nextday);
				if (exited) {
						double tradeProceeds=ticker.getExitPrice()*ticker.getBuyQty();
					

						totalCapital=totalCapital+tradeProceeds;
					
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
		System.err.println("Gapper "+obj.gapperCount);

		//System.err.println("Ideal buy point hit count "+obj.idealBuypointhit);
		
		
		for (Ticker trade : obj.trades) {
			double invested=trade.getExitPrice()*trade.getBuyQty();
			investedCapital=investedCapital+invested;
			
		}
	

		System.err.format("%f%n",investedCapital);
		System.err.format("%f%n",totalCapital);
		System.err.format("Total Capital "+"%f%n",investedCapital+totalCapital);



		System.err.println("Scalped Capital "+scalpedCapital);
		
		System.err.format("%f%n",scalpedCapital);
		
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

		//System.err.println("Win % " + totalwin + " Lost% " + totalloss);
		System.err.println("Scalp % " + (obj.scalpCount/totaltrades)*100);

		System.err.println("Average Win % " + totalwin / winners + " Lost% " + totalloss / losers);

		
		System.err.println("Total trades added :" + obj.tradecounter);
		System.err.println("Completed trades  :" + obj.completedtrades.size());
		System.err.println("Total Scalps  :" + obj.scalpCount);

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
