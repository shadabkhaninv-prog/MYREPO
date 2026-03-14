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
import java.util.StringJoiner;

public class FinalSimulator {
	Connection connection = null;
	private java.util.Date currentDay;
	ArrayList<Ticker> alltrades = new ArrayList<Ticker>();
	ArrayList<Ticker> tickers = new ArrayList<Ticker>();
	HashSet<Ticker> trades = new HashSet<Ticker>();
	ArrayList<Ticker> completedtrades = new ArrayList<Ticker>();
	HashSet<Ticker> neilwinners = new HashSet<Ticker>();
	
	private int tenpercents=0;
	
	ArrayList<String> mktdates = null;

	private int tradecounter = 0;
	private int squatters = 0;
	
	static final int TOTALHOLDINGS=10;
	
	private static final int stoplossLevel =6
			;
	
	
	private static final int laderlevel =stoplossLevel;

	private static final double trailstoplossLevel =15;
	private static final int scalpStoplevel=2;

	private static final int maxcapacity = TOTALHOLDINGS;
	private static int availableCapacity=TOTALHOLDINGS;
	public final static String tradehistory = "INSERT INTO `bhav`.`executionhistory`(`Symbol`,`Size`,`allocatedcapital`,`OpenDate`,`CloseDate`,`EntryPrice`,`ExitPrice`,`pnl`,`initialstoploss`,`trailer`,holdingperiod,returns,status,laddersl,past20,past15)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static double totalCapital=500000;

	private boolean firstAllocationDone=false;
	private double initialCapital=500000;
	private int initialAllocationCounter=0;
	
	private static double investedCapital=0;
	private static double scalpedCapital=0;
	private double averageroc;
	private int scalpCount=0;
	private double idealBuypointhit=0;
	
	private static final int threshold=5;
	
	private double slippage=0.25;
	
	
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
	


	
	private boolean checkAverageVolumes(Ticker ticker)throws SQLException,Exception{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String symbol=ticker.getSymbol();
		String mktdate=ticker.getMktdate();
		java.util.Date date = format.parse(mktdate);
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		String year = df.format(date);
		String bhavName = "bhav" + year;		
		String volumeCheckQuery="select avg(volume)*avg(close) as volume from "+bhavName+" where symbol=? and mktdate<? and mktdate>?-interval 20 day ;";
		//System.err.println(volumeCheckQuery);
		PreparedStatement selQuery;
		selQuery = connection
				.prepareStatement(volumeCheckQuery);
		selQuery.setString(1, symbol);
		selQuery.setString(2, mktdate);
		selQuery.setString(3, mktdate);
		
		ResultSet rs = selQuery.executeQuery();


		double volume=0;
		while (rs.next()) {
			volume=rs.getDouble("volume");
			
		}
		//System.err.println("Volume is  "+symbol+" is "+volume);
		ticker.setAvgVolume(volume);
		if(volume>1000000){
						return true;
		}else{
			System.err.println("Not adding Low Average volume for "+symbol+" is "+volume+" date "+mktdate);
		}
		return false;
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
	
	void loadTrades(String mktdate, int count) throws Exception{
		
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		String date = simpleDateFormat.format(new java.util.Date());
		System.out.println(date);
		
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

//			selQuery = connection
//					.prepareStatement("select symbol,mktdate,(roc3-roc1) as prevroc3,roc,roc63 from ttbuyalert where mktdate=? and roc1>=4 and roc>? and roc63>14 and roc15>0  having prevroc3<=3  order by roc desc limit " + count);
			
			StringBuilder sbSql = new StringBuilder( 1024 );
			
			
			
			
			
			// where symbol='LALPATHLAB' and opendate='2020-08-25
			
			
			
			
			
						sbSql.append( "SELECT * from bhav.ptaexecution where opendate>='2023-11-03' and opendate<='2023-12-08' order by OpenDate asc" );
			
			//where symbol='IOLCP' AND Opendate='2020-06-23'
			//sbSql.append( " )");
			//System.err.println("Query is "+sbSql.toString());
			 selQuery = connection.prepareStatement( sbSql.toString() );
			
			
			ResultSet rs = selQuery.executeQuery();
			Ticker ticker = null;
			while (rs.next()) {
				ticker = new Ticker();
				ticker.setSymbol(rs.getString("symbol"));
				ticker.setBuyDate(rs.getString("opendate"));
				ticker.setBuyPrice(rs.getDouble("EntryPrice"));
				//ticker.setBuyPrice(81.02);
				ticker.setBuyQty(rs.getDouble("size"));
				ticker.setAllocatedCapital(ticker.getBuyPrice()*ticker.getBuyQty());
				ticker.setStatus("open");
			//	ticker.setExitDate(rs.getString("closedate"));
				ticker.setProcessed(false);
				boolean added = trades.add(ticker);
				if (added){
					tradecounter++;
				}
			}
			
			Iterator<Ticker> iterator = trades.iterator();
			while (iterator.hasNext()) {
				int runcount=0;

				ticker = iterator.next();
				loadMarketDates(ticker.getBuyDate(), date);	

				if(mktdates.size()>=150){
					runcount=150;
				}
				else{
					runcount=mktdates.size();
				}
				for (int i = 0; i < runcount; i++) {
				//	System.err.println(mktdates.size()+" "+i);
					String today = mktdates.get(i);
			//		String nextday = getNextDay(today);
					
					System.err.println("Going in to "+today+" initial Stoploss is "+ticker.getStopLoss()+" Trailer is "+ticker.getTrailer());

				boolean exited =simulateTrade(ticker, today,i);
				if(exited) {
					ticker.setStatus("Closed");
					break;
				}
				}

			}
		

		} catch (SQLException e) {
			e.printStackTrace();
		}
		 System.out.println("Tickers "+trades.size());

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
			ticker.setTrailer(ticker.getTrailer() / ratio);
			ticker.setHighest(ticker.getHighest()/ratio);



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
			ticker.setHighest(ticker.getHighest()/ratio);
			ticker.setTrailer(ticker.getTrailer() / ratio);


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
	
	private double getPreviousDayLow(String ticker,String mktdate,String bhav){
		double prevlow=0;
		String query="select low as prevlow from "+ bhav +" where symbol=? and  mktdate=? ";
		PreparedStatement selQuery;
		boolean exited = false;
		

		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker);
			selQuery.setString(2, mktdate);
			ResultSet rs = selQuery.executeQuery();
			while (rs.next()) {
				prevlow = rs.getDouble("prevlow");

			}
	//		System.err.println("Rally lowe is "+rallylow+" for "+ticker+" from mktdate "+mktdate);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return prevlow;

	}
	
	
	
	private double getRallyLow(String ticker,String mktdate,String fivedaysback,String bhav){
		double rallylow=0;
		String query="select min(low) as rallylow from "+ bhav +" where symbol=? and  mktdate<=? and mktdate>=?";
		PreparedStatement selQuery;
		boolean exited = false;
		

		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker);
			selQuery.setString(2, mktdate);
			selQuery.setString(3, fivedaysback);
			ResultSet rs = selQuery.executeQuery();
			while (rs.next()) {
				rallylow = rs.getDouble("rallylow");

			}
	//		System.err.println("Rally lowe is "+rallylow+" for "+ticker+" from mktdate "+mktdate);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return rallylow;

	}
	
	private double getTendayshigh(String ticker,String mktdate,String tendaysback,String bhav){
		double tendayshigh=0;
		String query="select max(high) as tendayshigh from "+ bhav +" where symbol=? and  mktdate<=? and mktdate>=?";
		PreparedStatement selQuery;
		boolean exited = false;
		

		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker);
			selQuery.setString(2, mktdate);
			selQuery.setString(3, tendaysback);
			ResultSet rs = selQuery.executeQuery();
			while (rs.next()) {
				tendayshigh = rs.getDouble("tendayshigh");

			}
	//		System.err.println("Rally lowe is "+rallylow+" for "+ticker+" from mktdate "+mktdate);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return tendayshigh;

	}
		
	
	boolean simulateTrade(Ticker ticker, String mktdate,int holdingperiod) throws SQLException, Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	//	System.err.println(mktdate);
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
			
			boolean exitTomorrow=ticker.isExitTommorow();

			double exitPrice = 0;
			double scalpPrice=0;
			double scalpStop=0;

			int helddays = 0;
			while (rs.next()) {
			

				//double dma10 = 0;

				String closeindictor=rs.getString("closeindictor");
				
				// ticker=new Ticker();
				helddays = ticker.getHeldDays() + 1;
				ticker.setHeldDays(helddays);
				ticker.setPositiveIndictor(closeindictor);
				
				double close = rs.getDouble("close");
				ticker.setClose(close);
				double open = rs.getDouble("open");

				double high = rs.getDouble("high");
				double low = rs.getDouble("low");
				double idealBuyprice=0;
				double prevclose=rs.getDouble("prevclose");


				double volatility=rs.getDouble("volatility");
				if (!ticker.isProcessed()) {
					System.err.println("Processing for first time "+ticker+" "+mktdate);
					double bodayLow=rs.getDouble("low");
					ticker.setPrevclose(prevclose);
					String prevday=TradeUtil.getPreviousDay(ticker.getBuyDate(), connection);
					String tendaysback=TradeUtil.getTenDayBack(ticker.getBuyDate(), connection);
					double prevlow=getPreviousDayLow(ticker.getSymbol(), prevday, bhavName);

					double lowday10=getPreviousDayLow(ticker.getSymbol(), tendaysback, bhavName);
				
					//System.err.println(ticker.getSymbol()+" Buy price "+idealBuyprice+" Close "+buyPrice+" On "+ticker.getBuyDate());
					//System.err.println(ticker.getSymbol()+" IDeal Buy price "+idealBuyprice+" Pivot price "+ticker.getPivotBuyPrice()+" On "+ticker.getBuyDate());
					//stoploss = ticker.getBuyPrice() - ticker.getBuyPrice() * stoplossLevel / 100;
					stoploss = prevlow - prevlow * 0.2 / 100;
					
					if(stoploss>=ticker.getBuyPrice())
						stoploss = bodayLow - bodayLow * 0.2 / 100;


					
					//stoploss = lowday10;

				//	scalpStop=buyPrice - buyPrice * scalpStoplevel / 100;
					//System.err.println(ticker.getSymbol()+" Scalp target is : "+scalpPrice);
					//stoploss=Math.round(stoploss*100.0)/100.0;
					//System.err.println("Stop loss is "+stoploss);
					ticker.setStopLoss(stoploss);
						ticker.setInitialstop(stoploss);					
						ticker.setProcessed(true);

					// System.out.println("BuyPrice : "+buyPrice+" Stoploss
					// "+stoploss);
				}
				
				if(helddays==1){
					return false;
				}
				
				if(helddays>1){
					double percentrise = 100 * ((close - buyPrice) / buyPrice);
					double firstReactionClose=0;
					if(closeindictor.equals("N")){
						System.err.println(ticker.getSymbol()+" reaction has occured "+close+" on "+mktdate);
						if(close>ticker.getReactionClose()){
						ticker.setReactionClose(close);
						}
					}

				if(ticker.getHighest()==0){
					ticker.setHighest(high);
					}
				
					if(ticker.getHighest()<high){
					//	System.err.println(ticker.getSymbol()+" Volatility high at new high "+volatility+" indictor"+closeindictor);
						ticker.setHighest(high);

//							}
//						}
					}
					

			
				}
				// Handle bonus and splits
				 prevclose = rs.getDouble("prevclose");
				double diffFromPrev = 100 * ((close - prevclose) / prevclose);
				if (diffFromPrev < -10) {
					double adjustedPrice = getAdjustedPrice(ticker, mktdate, ticker.getBuyPrice());
					if (adjustedPrice > 0) {

						buyPrice = ticker.getBuyPrice();
					}

				}
				
			
				
			
				if(low<ticker.getStopLoss()){
					//System.err.println("Stoploss hit "+ticker+" "+ticker.getStopLoss()+" ON "+mktdate);
					ticker.setExitDate(mktdate);
					ticker.setExitPrice(ticker.getStopLoss()-ticker.getStopLoss()*0.25/100);
					//ticker.setExitPrice(close-close*0.25/100);

					ticker.setHeldDays(holdingperiod);
					System.err.println("eXITED "+ticker+" "+ticker.getStopLoss()+" ON "+mktdate);

					
					return true;
				}

				if(low<ticker.getTrailer()){
					//System.err.println("Stoploss hit "+ticker+" "+ticker.getStopLoss()+" ON "+mktdate);
					ticker.setExitDate(mktdate);
					ticker.setExitPrice(ticker.getTrailer()-ticker.getTrailer()*0.25/100);
					//ticker.setExitPrice(close-close*0.25/100);

					ticker.setHeldDays(holdingperiod);

					System.err.println("Trailer hit eXITED "+ticker+" "+ticker.getTrailer()+" ON "+mktdate);

					
					return true;
				}
				
				if(helddays>=200){
					ticker.setExitDate(mktdate);
					ticker.setExitPrice(close);
					ticker.setHeldDays(helddays);

					System.err.println("Holding period over "+ticker+" "+close+" ON "+mktdate);
					return true;

				}
				
			
				//
				
			
				double percentdiff = 100 * ((close - buyPrice) / buyPrice);
				// percentdiff = 100 * ((ticker.getHighest() - buyPrice) / buyPrice);

//				
			
			

				if(percentdiff>=15){
				//	System.err.println("Crossed 10 percent on closing basis "+ticker);
					ticker.setPast15(true);

				}
				
		
				if(percentdiff>=threshold && !ticker.hasPassThreshold()){
				//	System.err.println("Crossed 10 percent on closing basis "+ticker);
					ticker.setPassThreshold(true);
					ticker.setTrailer(ticker.getBuyPrice()+ticker.getBuyPrice()*0.5/100);
					
					System.err.println("Crossed 10 percent on closing basis "+ticker.getTrailer());
					
				}
				
				if(percentdiff>=trailstoplossLevel){
					//System.err.println("Crossed 15 percent on closing basis "+ticker);
					ticker.setPassThreshold2(true);
					// set stoploss to below 2% below tendays high
					String tendaysback=TradeUtil.getTenDayBack(ticker.getBuyDate(), connection);
					
					double tendayshigh=getTendayshigh(ticker.getSymbol(), ticker.getBuyDate(), tendaysback, bhavName);
					double newtrailer=ticker.getBuyPrice()+ticker.getBuyPrice()*2/100;
					// newtrailer=ticker.getHighest()-ticker.getHighest()*15/100;
					newtrailer=close-close*trailstoplossLevel/100;
					ticker.setPast20(true);

					if(newtrailer>ticker.getTrailer()){
					ticker.setTrailer(newtrailer);
					System.err.println("post 20 percent :Buy price :"+ticker.getBuyPrice()+" Trailer is : "+ticker.getBuyPrice()+ticker.getBuyPrice()*2);

					}

					//System.err.println("Crossed 15 percent on closing basis Ten days high"+tendayshigh+" New trailer is "+ticker.getTrailer());
					
					
					
				}
				

//				if(helddays>20 && percentdiff<=0 ){
//					System.err.println("Dud  eXITED "+ticker+" "+ticker.getBuyPrice()+" ON "+mktdate);
//
//					ticker.setExitDate(mktdate);
//					ticker.setExitPrice(close-close*0.25/100);
//					ticker.setHeldDays(holdingperiod);
//
//					return true;
//					
//
//			
//				}
//				if(helddays>40 && percentdiff<=10 ){
//					System.err.println("Dud  eXITED "+ticker+" "+ticker.getBuyPrice()+" ON "+mktdate);
//
//					ticker.setExitDate(mktdate);
//					ticker.setExitPrice(close-close*0.25/100);
//					ticker.setHeldDays(holdingperiod);
//
//					return true;
//					
//
//			
//				}
//						
//				if(helddays>60 && percentdiff<=30 ){
//					System.err.println("Dud  eXITED "+ticker+" "+ticker.getBuyPrice()+" ON "+mktdate);
//
//					ticker.setExitDate(mktdate);
//					ticker.setExitPrice(close-close*0.25/100);
//					ticker.setHeldDays(holdingperiod);
//
//					return true;
//					
//
//			
//				}

			

				
	
	

			

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
	// 	public final static String tradehistory = 
	//public final static String tradehistory = 
	//public final static String tradehistory = "INSERT INTO `bhav`.`executionhistory`(`Symbol`,`Size`,`allocatedcapital`,`OpenDate`,`CloseDate`,
	//`EntryPrice`,`ExitPrice`,`Returns`,`initialstoploss`,`trailer`)
	// VALUES(?,?,?,?,?,?,?,?,?,?,)";


	private void insertexecutionHistory() throws SQLException, Exception {
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		st.execute("truncate table bhav.executionhistory");

		PreparedStatement prep = connection.prepareStatement(tradehistory);

		for (Ticker trade : trades) {
			if(trade.getStatus().equalsIgnoreCase("open")){
				trade.setExitPrice(trade.getClose());
			}
					prep.setString(1, trade.getSymbol());
			prep.setDouble(2, trade.getBuyQty());
			prep.setDouble(3, Math.round(trade.getAllocatedCapital()));

			prep.setString(4, trade.getBuyDate());
			
			
			prep.setString(5, trade.getExitDate());
			prep.setDouble(6,trade.getBuyPrice());
			prep.setDouble(7, trade.getExitPrice());
			double netreturns=trade.getBuyQty()*(trade.getExitPrice()-trade.getBuyPrice());
			prep.setDouble(8,netreturns);

//			double 
//			
//		    double proportionCorrect = ((float) correct) / ((float) questions);


			
	
			prep.setDouble(9, trade.getInitialstop());
			prep.setDouble(10, trade.getTrailer());
			prep.setInt(11, trade.getHeldDays());
			
			prep.setDouble(12, 0);
			
			prep.setString(13, trade.getStatus());
			prep.setDouble(14, trade.getLaddersl());

prep.setString(15, trade.isPast20()?"Y":"N");
prep.setString(16, trade.isPast15()?"Y":"N");



	

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
		if(initialAllocationCounter==TOTALHOLDINGS){
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
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		String date = simpleDateFormat.format(new java.util.Date());
		System.out.println(date);
		
		FinalSimulator obj = new FinalSimulator();
		String startdate = "2023-11-01";
		String enddate = "2023-12-08";
		enddate =date;
		obj.connect();
		//obj.doVCP();
		obj.loadMarketDates(startdate, enddate);
		
		
		int count = maxcapacity;
		
		
		double totalearned = 0;
		//double allocation=capital/8;
		// obj.currentDay=new
		// java.util.Date(formatter.parse(buydate).getTime());

		System.err.println("Market dates " + obj.mktdates.size());
	

		for (int i = 0; i < 1; i++) {
			
			String today = obj.mktdates.get(i);
			String nextday = obj.getNextDay(today);
			obj.loadTrades(today, count);
			// System.err.println("trading on "+today);
//			Iterator<Ticker> iterator = obj.trades.iterator();
//			while (iterator.hasNext()) {
//				Ticker ticker = iterator.next();
//
//				boolean exited = obj.simulateTrade(ticker, nextday);
//				if (exited) {
//						double tradeProceeds=ticker.getExitPrice()*ticker.getBuyQty();
//						double sumProceeds=tradeProceeds+ticker.getScalpedAmount();	
//					//	System.err.println(ticker.getSymbol()+" Trade proceeds "+tradeProceeds+" ScalpProceeds "+ticker.getScalpedAmount()+" Total : "+sumProceeds);
//						//tradeProceeds=sumProceeds;
//						//System.err.println("Capital "+totalCapital);
//
//						totalCapital=totalCapital+tradeProceeds;
//						//System.err.println(ticker+" exited "+tradeProceeds+" Capital "+totalCapital);
//					
//					obj.completedtrades.add(ticker);
//					// System.err.println(ticker.getSymbol()+" exited
//					// "+ticker.getExitDate());
//					iterator.remove();
//					obj.availableCapacity++;
//
//				}
//
//			}
//			
//			obj.calculateEOD(nextday);

		}
	

//		obj.alltrades.addAll(obj.completedtrades);
//		obj.alltrades.addAll(obj.trades);
//		System.err.println("Completed Trades " + obj.completedtrades.size());
//		System.err.println("Open Trades " + obj.trades.size());
//		System.err.println("Ideal Buy point " + obj.idealBuypointhit);
//
//		System.err.println("AvailableCapacity "+availableCapacity);
//		System.err.println("Available Capital "+totalCapital);
//		

		//System.err.println("Ideal buy point hit count "+obj.idealBuypointhit);
		
		System.err.println("Total trades"+obj.trades.size());

	//	System.err.println("Total Ten percenters "+obj.tenpercents);
//		System.err.println("Invested Capital "+investedCapital);
//		
//		System.err.format("%f%n",investedCapital);
//		
//		System.err.format("Total Capital "+"%f%n",investedCapital+totalCapital);
//
//		System.err.println("Scalped Capital "+scalpedCapital);
//		
//		System.err.format("%f%n",scalpedCapital);
//		
		for (Ticker trade : obj.trades) {
			if(trade.getStatus().equalsIgnoreCase("open")){
				trade.setExitPrice(trade.getClose());
			}
			double exitprice = trade.getExitPrice();
			double buyprice = trade.getBuyPrice();
			double Bought = trade.getBuyQty();
			double investedamount=Bought*buyprice;
			double Sold = exitprice * Bought;
			double gainloss = Sold - (Bought * buyprice);
			gainloss = Math.round(gainloss * 100.0) / 100.0;
			double Result = investedamount + gainloss;
			StringJoiner winnerstring=new StringJoiner(",");
			if (exitprice >= buyprice) {
				winners++;
				double gain = 100 * ((exitprice - buyprice) / buyprice);
				double profit = exitprice - buyprice;
				winnerstring.add(trade.getSymbol());
				winnerstring.add(trade.getBuyDate());
				winnerstring.add(Double.toString(trade.getBuyPrice()));
				winnerstring.add(Double.toString(trade.getExitPrice()));
				winnerstring.add(trade.getExitDate());
				winnerstring.add(Double.toString(trade.getBuyQty()*(trade.getExitPrice()-trade.getBuyPrice())));
				totalwin = totalwin + gain;
			//	System.out.println(winnerstring);
//				System.out.println(trade.getSymbol() + " Bought on " + trade.getBuyPrice() + " on " + trade.getBuyDate()
//						+ "  Exit Price " + trade.getExitPrice() + " on " + trade.getExitDate() + " Gain :" + gainloss
//						+ " Held :" + trade.getHeldDays() + " Days" + " Invested:" + investedamount + " Stop loss: "
//						+ trade.getStopLoss());
			} else {
				losers++;
				double loss = 100 * ((exitprice - buyprice) / buyprice);
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
		obj.insertexecutionHistory();
//		obj.insertEODStatus();
//		double totaltrades=winners+losers;
//		
		double perwin=winners / obj.trades.size();
		perwin *=100;
//		
		System.err.println("Winners " + winners + " Losers " + losers+" total "+obj.trades.size());
		System.err.println("Win % "+perwin +" Loss "+(losers/obj.trades.size())*100);
//		//System.err.println("Avg Holding period :" + holdingperiod / obj.completedtrades.size() + " Days");
//
//		System.err.println("Win % " + totalwin + " Lost% " + totalloss);
//		System.err.println("Scalp % " + (obj.scalpCount/totaltrades)*100);
//
		System.err.println("Average Win % " + totalwin / winners + " Lost% " + totalloss / losers);
//
//		
//		System.err.println("Total trades added :" + obj.tradecounter);
//		System.err.println("Completed trades  :" + obj.completedtrades.size());
//		System.err.println("Total Scalps  :" + obj.scalpCount);
//
//		System.err.println("Open trades  :" + obj.trades.size());
//		System.err.println("Squatters  :" + obj.squatters);
//		System.err.println("Open Trade  :" + obj.trades);
//////
//////		System.err.println("Neil winners  :" + obj.neilwinners.size());
//////		for (Ticker trade : obj.neilwinners) {
//////			System.err.println(
//////					trade + " " + trade.getBuyPrice() + " " + trade.getExitPrice() + " Gain " + trade.getPercentGain());
//////		}
		
	}

}
