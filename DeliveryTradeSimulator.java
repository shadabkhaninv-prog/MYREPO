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

public class DeliveryTradeSimulator {
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
	
	static final int TOTALHOLDINGS=8;
	
	private static final int stoplossLevel =5
			;
	private static final int scalplevel =4;

	private static final double trailstoplossLevel =4;
	private static final int scalpStoplevel=2;

	private static final int maxcapacity = TOTALHOLDINGS;
	private static int availableCapacity=TOTALHOLDINGS;
	public final static String tradehistory = "INSERT INTO `bhav`.`tradehistory`(`symbol`,`buydate`,`buyqty`,`buyprice`,`allocatedcapital`,`exitprice`,`exitdate`,`stoplosshit`,`stoplossprice`,squatted,holdingperiod,neilwinner,exittomorrow,roc,roc63,prevclose,highest,scalpprice,scalped,scalpprofit,pivotbuypoint)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static double totalCapital=500000;

	private boolean firstAllocationDone=false;
	private double initialCapital=500000;
	private int initialAllocationCounter=0;
	
	private static double investedCapital=0;
	private static double scalpedCapital=0;
	private double averageroc;
	private int scalpCount=0;
	private double idealBuypointhit=0;
	
	
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
	
	
private boolean isDeliveryHighEnough(String mktdate,String symbol) throws SQLException,Exception{
	
SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	java.util.Date date = format.parse(mktdate);
	SimpleDateFormat df = new SimpleDateFormat("yyyy");
	String year = df.format(date);
	String bhavName = "bhav" + year;
	
	String lookBackDate=TradeUtil.getTenDayBack(mktdate, connection);
	double todaysvolume=TradeUtil.getMaxDeliveryVolume(lookBackDate, mktdate,symbol, connection, bhavName);
	double deliverydetails[]=TradeUtil.getTodayDeliveryVolume(mktdate, symbol, connection, bhavName);
double maxvolume=deliverydetails[0];
double deliverypercent=deliverydetails[1];


	if(todaysvolume==maxvolume){
		System.err.println(symbol+" Has recorded highest delivery volume on "+mktdate+" "+todaysvolume+" ");
		return true;
	}
	return false;
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
	
	void loadBuyAlerts(String mktdate, int count) throws Exception{
		
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
			
			StringBuilder sbSql = new StringBuilder( 1024 );
			sbSql.append( "select symbol,mktdate,roc,roc63,buypoint,round(((close-buypoint)/buypoint)*100,2) as diff from ttbuyalert where symbol!='TIPSINDLTD' and avgvol*close>1000000 and mktdate=? and roc63>10 and roc>?0 and symbol not in(" );
			
			String[] Parameter;
			Parameter=new String[trades.size()];
			if(!trades.isEmpty()){
				int p=0;
				for(Ticker t:trades){
				
					//System.err.println("Loading parameters .."+p);
		
				//System.err.println(t.getSymbol());
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
			sbSql.append( " )  order by roc desc limit " + count);
			//System.err.println("Query is "+sbSql.toString());
			 selQuery = connection.prepareStatement( sbSql.toString() );
			
			selQuery.setString(1, mktdate);
			selQuery.setDouble(2, 0);
			if(trades.isEmpty()){
				selQuery.setString(3,"ignore");
			}
			for( int i=0; i < Parameter.length; i++ ) {
				selQuery.setString( i+3, Parameter[ i ] );
			} // for
			
//			selQuery = connection
//					.prepareStatement("select symbol,mktdate,roc,roc63 from ttbuyalert where mktdate=? and roc63>15 and roc>? and symbol not in ?  order by roc desc limit " + count);
//	
		
			//selQuery.setArray(3, array);
			//selQuery.setDouble(3, 60);
			//selQuery.setDouble(4, 0);
			
			ResultSet rs = selQuery.executeQuery();
			Ticker ticker = null;
			while (rs.next()) {
				ticker = new Ticker();
				ticker.setSymbol(rs.getString("symbol"));
				ticker.setMktdate(rs.getString("mktdate"));
				ticker.setRoc(rs.getDouble("roc"));
				ticker.setRoc63(rs.getDouble("roc63"));
				ticker.setPivotBuyPrice(rs.getDouble("buypoint"));
				
			if(isDeliveryHighEnough(mktdate, ticker.getSymbol())){
				
				boolean added = trades.add(ticker);
				if (added){
					tradecounter++;
				}
				}
			else{
				
				System.err.println("Continuing to loop "+mktdate);
				if(mktdate.equals("2020-02-17")){
					System.err.println("Processing to loop "+mktdate);

				}
				continue;
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
	
	private boolean isIdealPriceReached(String ticker,String mktdate,double IdealBuyPrice) throws Exception{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date date = format.parse(mktdate);
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		String year = df.format(date);
		String bhavName = "bhav" + year;
		String query = "select symbol,mktdate,low from " + bhavName
				+ " where symbol=? and mktdate=?";
		PreparedStatement selQuery;
		boolean exited = false;
		selQuery = connection.prepareStatement(query);

		ResultSet rs = selQuery.executeQuery();
		while (rs.next()) {
			double low = rs.getDouble("low");
			if(low<IdealBuyPrice){
				return true;
			}
		}
		
		
		
		return false;
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
			
			boolean exitTomorrow=ticker.isExitTommorow();

			double exitPrice = 0;
			double scalpPrice=0;
			double scalpStop=0;

			int helddays = 0;
			while (rs.next()) {
				double dma50 = rs.getDouble("50dma");
				double dma20 = rs.getDouble("20dma");
				double dma200 = rs.getDouble("200dma");
				double dma10 = 0;

				//double dma10 = 0;

				String closeindictor=rs.getString("closeindictor");
				
				// ticker=new Ticker();
				helddays = ticker.getHeldDays() + 1;
				ticker.setHeldDays(helddays);
				ticker.setPositiveIndictor(closeindictor);
				ticker.setDma20(dma20);
				ticker.setDma10(dma10);

				double close = rs.getDouble("close");
				double open = rs.getDouble("open");

				double high = rs.getDouble("high");
				double low = rs.getDouble("low");
				double idealBuyprice=0;

				double volatility=rs.getDouble("volatility");
				if (ticker.getBuyDate() == null) {
					double bodayLow=rs.getDouble("low");
					double prevclose=rs.getDouble("prevclose");
					ticker.setPrevclose(prevclose);
					
					buyPrice = close;
					//reduce by 1 percent
					//idealBuyprice =close- close*1/100;
					//ticker.setIdealBuyPoint(idealBuyprice);
					
					//idealBuyprice =ticker.getPivotBuyPrice()+ ticker.getPivotBuyPrice()*2/100;
					//buyPrice=idealBuyprice;
					//ticker.setBuyPrice(idealBuyprice);
					ticker.setBuyPrice(buyPrice);
					ticker.setBodayLow(bodayLow);
					ticker.setBuyDate(mktdate);
					ticker.setIdealBuyPoint(idealBuyprice);
					//System.err.println(ticker.getSymbol()+" Buy price "+idealBuyprice+" Close "+buyPrice+" On "+ticker.getBuyDate());
					//System.err.println(ticker.getSymbol()+" IDeal Buy price "+idealBuyprice+" Pivot price "+ticker.getPivotBuyPrice()+" On "+ticker.getBuyDate());
					stoploss = buyPrice - buyPrice * stoplossLevel / 100;
					scalpPrice=buyPrice + buyPrice * scalplevel / 100;
				//	scalpStop=buyPrice - buyPrice * scalpStoplevel / 100;
					//System.err.println(ticker.getSymbol()+" Scalp target is : "+scalpPrice);
					//stoploss=Math.round(stoploss*100.0)/100.0;
					ticker.setStopLoss(stoploss);
					ticker.setScalpPrice(scalpPrice);
					//ticker.setScalpStoploss(scalpStop);
					getAllocation(ticker);
					availableCapacity=availableCapacity-1;

					// System.out.println("BuyPrice : "+buyPrice+" Stoploss
					// "+stoploss);
				}
		
				if(helddays>1){
					double percentrise = 100 * ((close - buyPrice) / buyPrice);
					

				if(ticker.getHighest()==0){
					ticker.setHighest(high);
					}
				
					if(ticker.getHighest()<high){
					//	System.err.println(ticker.getSymbol()+" Volatility high at new high "+volatility+" indictor"+closeindictor);
						ticker.setHighest(high);
//						if(closeindictor.equalsIgnoreCase("N") && volatility>12){
//							System.err.println(ticker.getSymbol()+" mktdate "+mktdate+" Volatility high at new high . exit!");
//							ticker.setExitTommorow(true);
//							
//						}
						//Add check for maximum volume ever!
//						if(closeindictor.equalsIgnoreCase("N")){
//							//System.err.println(ticker.getSymbol()+" mktdate "+mktdate+" New Hihh and closed negative!");
//							double avgvolatility=getAverageVolatility(ticker.getSymbol(),mktdate,bhavName);
//							if(volatility>avgvolatility*1.5 && volatility>=10){
//								//System.err.println(mktdate+" - Volatiltiy is out of ball park "+ticker.getSymbol()+" exit"+volatility);
//								//getMaxVolume(ticker.getSymbol(),mktdate,bhavName);
//								//ticker.setExitTommorow(true);
//							}
//						}
					}
					

					if(ticker.getScalpPrice()<high && ticker.isScalped()==false){
						double scalpQty=Math.round(ticker.getBuyQty()*20/100);
						
						//System.err.println(ticker.getSymbol()+" "+ticker.getBuyQty()+" - "+scalpQty+" - Scalped at "+ticker.getScalpPrice());
						ticker.setScalpProfit(scalpQty*ticker.getScalpPrice()-scalpQty*ticker.getBuyPrice());
						ticker.setScalpedAmount(scalpQty*ticker.getScalpPrice());
						scalpedCapital=scalpedCapital+ticker.getScalpProfit();
						ticker.setScalped(true);
						//System.err.println("Before Scalping .."+ticker.getSymbol()+" "+ticker.getBuyQty());

						ticker.setScalpedDay(helddays);
						//ticker.setBuyQty(ticker.getBuyQty()-scalpQty);
					//	System.err.println("After scalping "+ticker.getSymbol()+" "+ticker.getBuyQty());

						scalpCount++;
					}
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
//				
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
				
		
				
				
				
				if (percentdiff >trailstoplossLevel) {

					double trailstoploss = close - close * trailstoplossLevel / 100;
				
					
					ticker.setPassThreshold(true);

					
					
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
				
//			if(percentdiff>=25){
//				ticker.setExitDate(mktdate);
//				System.err.println("REached threshold exiting");
//				exited=true;
//				break;
//			}
			
				if (close < ticker.getStopLoss()) {
					// System.out.println("Exiting stoploss reached "+mktdate +"
					// close "+close+" stoploss "+ticker.getStopLoss());
					ticker.setExitDate(mktdate);
					exited = true;
					//ticker.setExitPrice(ticker.getStopLoss());
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
				
//				if (close <ticker.getDma10()) {
//					 System.out.println(ticker+" Exiting 10dma reached "+mktdate +" close "+close+" stoploss "+ticker.getDma10());
//					ticker.setExitDate(mktdate);
//					exited = true;
//					//ticker.setExitPrice(ticker.getStopLoss());
//					ticker.setStoplossHit(true);
//
//					// trades.add(ticker);
//					break;
//
//				}
				
				if (helddays > 10 && percentdiff < -4	) {
					//System.err.println(ticker+" 20 day Falling more than 5 percent "+percentdiff);
					ticker.setExitTommorow(true);
					ticker.setSquatted(true);
					System.err.println("Cutting loser " + ticker.getSymbol() + " Squatted!");
					break;
				}
//	
				if (helddays > 20 && percentdiff < -2	) {
					//System.err.println(ticker+" 20 day Falling more than 5 percent "+percentdiff);
					ticker.setExitTommorow(true);
					ticker.setSquatted(true);
					System.err.println("Cutting loser " + ticker.getSymbol() + " Squatted!");
					break;
				}
				if (helddays > 30 && percentdiff < 10) {
					//System.err.println(ticker+" 20 day Falling more than 5 percent "+percentdiff);
					ticker.setExitTommorow(true);
					ticker.setSquatted(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}

				if ((helddays > 40) && percentdiff < 20) {
					ticker.setExitTommorow(true);
					System.err.println("Exiting " + ticker.getSymbol() + " Squatted!"+percentdiff+mktdate);
					break;
				}
//				
//
				if ((helddays > 75) && percentdiff < 50) {
					ticker.setExitTommorow(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}

				if ((helddays > 100) && percentdiff < 75) {
					ticker.setExitTommorow(true);
					//System.err.println("Exiting " + ticker.getSymbol() + " Squatted!");
					break;
				}
//				

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
			prep.setDouble(21, trade.getPivotBuyPrice());

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
		
		DeliveryTradeSimulator obj = new DeliveryTradeSimulator();
		String startdate = "2020-01-01";
		String enddate = "2015-12-31";
		enddate =date;
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
			obj.loadBuyAlerts(today, count);
			// System.err.println("trading on "+today);
			Iterator<Ticker> iterator = obj.trades.iterator();
			while (iterator.hasNext()) {
				Ticker ticker = iterator.next();

				boolean exited = obj.simulateTrade(ticker, nextday);
				if (exited) {
						double tradeProceeds=ticker.getExitPrice()*ticker.getBuyQty();
						double sumProceeds=tradeProceeds+ticker.getScalpedAmount();	
					//	System.err.println(ticker.getSymbol()+" Trade proceeds "+tradeProceeds+" ScalpProceeds "+ticker.getScalpedAmount()+" Total : "+sumProceeds);
						//tradeProceeds=sumProceeds;
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
		System.err.println("Ideal Buy point " + obj.idealBuypointhit);

		System.err.println("AvailableCapacity "+availableCapacity);
		System.err.println("Available Capital "+totalCapital);
		

		//System.err.println("Ideal buy point hit count "+obj.idealBuypointhit);
		
		
		for (Ticker trade : obj.trades) {
			double invested=trade.getExitPrice()*trade.getBuyQty();
			investedCapital=investedCapital+invested;
			
		}
		System.err.println("Invested Capital "+investedCapital);
		
		System.err.format("%f%n",investedCapital);
		
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

		System.err.println("Win % " + totalwin + " Lost% " + totalloss);
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
