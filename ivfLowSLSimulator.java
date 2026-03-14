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

public class ivfLowSLSimulator {
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

	static final int TOTALHOLDINGS = 6
			;

	private static final double stoplossLevel = 1;
	private static final double surgeFactor =2;

	private static final double trailstoplossLevel = 10;

	private static final int scalplevel = 4;

	private static final int maxcapacity = TOTALHOLDINGS;
	private static int availableCapacity = TOTALHOLDINGS;
	public final static String tradehistory = "INSERT INTO `bhav`.`cantradehistory`(`symbol`,`buydate`,`buyqty`,`buyprice`,`allocatedcapital`,`exitprice`,`exitdate`,`stoplosshit`,`stoplossprice`,squatted,holdingperiod,neilwinner,exittomorrow,roc,roc63,prevclose,highest,scalpprice,scalped,scalpprofit,lastvolume,lasthigh,surgefactor)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static double totalCapital = 500000;

	private boolean firstAllocationDone = false;
	private double initialCapital = 500000;
	private int initialAllocationCounter = 0;

	private static double investedCapital = 0;
	private static double scalpedCapital = 0;
	private double averageroc;
	private int scalpCount = 0;
	private double gapperCount = 0;
	private double pivotPlus = Constants.pivotPlus;
	private double samedaystoplosslevel = 3;

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

	// public static final String averagerocquery="select avg(roc63) as
	// averageroc63,avg(roc) as averageroc from ttbuyalert where mktdate<=? and
	// mktdate>=date_add(?,interval -180 day)";
	public static final String averagerocquery = "select lookbackroc from portfolio.corpus where AsOf=DATE_SUB(LAST_DAY(?),INTERVAL DAY(LAST_DAY(?))-1 DAY)";

	// checks trend

	void loadCandidates(String mktdate) throws Exception {
		PreparedStatement selQuery;
		ResultSet rs = null;
		int existingtrades = trades.size();

		if (existingtrades == maxcapacity) {
			// System.err.println("Fully allocated Portfolio");
			return;

		}

		try {
			// System.err.println("Loading candidates for "+mktdate);
			StringBuilder sbSql = new StringBuilder(1024);
			sbSql.append(
					"select *,round(((volatility-avgvolatility)/avgvolatility)*100,2) as delta from ivflist where TC10DAY>5 and rise52weeklow>=30 and from52weekhigh>=-10 and from52weekhigh<0 and dayssincehigh>15 and mktdate=? and symbol not in(");

			String[] Parameter;
			Parameter = new String[trades.size()];
			if (!trades.isEmpty()) {
				int p = 0;
				for (Ticker t : trades) {

					// System.err.println("Loading parameters .."+p);

					// System.err.println(t.getSymbol());
					Parameter[p] = t.getSymbol();
					p++;
				}
				// System.err.println("Parameter "+Parameter.toString());
				for (int i = 0; i < Parameter.length; i++) {
					if (i > 0)
						sbSql.append(",");
					sbSql.append(" ?");
				}
			} // for
			else {
				sbSql.append(" ?");
			}
			sbSql.append(
					" )  and volatility<avgvolatility  and symbol not REGEXP 'SNOWMAN|NIFTY|NSE|ETF|BEES|GOLD|RELBANK|ICICINXT50' having delta<=-35  order by rise52weeklow desc,TC10DAY desc limit 15 ");
			// System.err.println("Query is "+sbSql.toString());
			selQuery = connection.prepareStatement(sbSql.toString());

			selQuery.setString(1, mktdate);
			// selQuery.setDouble(2, 0);
			if (trades.isEmpty()) {
				selQuery.setString(2, "ignore");
			}
			for (int i = 0; i < Parameter.length; i++) {
				selQuery.setString(i + 2, Parameter[i]);
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
				// ticker.setMktdate(rs.getString("mktdate"));
				ticker.setPivotBuyPrice(rs.getDouble("10dayhigh"));
				ticker.setBreakoutVolume(rs.getDouble("21dayavgvolume"));
				ticker.setLastVolume(rs.getDouble("dayvolume"));
				ticker.setAvgVolatility(rs.getDouble("avgvolatility"));
				ticker.setLow10day(rs.getDouble("10daylow"));
				String tradingday = getNextDay(mktdate);

				boolean breakout = breakoutSimulation(tradingday, ticker);
				if (breakout) {

					// System.err.println("Taking trade post breakout");
					boolean added = trades.add(ticker);
					if (added) {
						tradecounter++;
						// System.err.println("Taking trade post breakout
						// "+ticker.getSymbol()+" on "+tradingday);
					}
				}

			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			rs.close();
			rs = null;
		}
		// System.out.println("Tickers "+trades.size());

	}

	private int checkVolumePercentile(Ticker ticker, String newbhav, String oldbhav, String mktdate, double volume)
			throws Exception {
		String volumePercentilQuery = "select count(*) as rank from(select  symbol,mktdate,volume  from " + oldbhav
				+ "  WHERE mktdate>=?-interval 1 year and symbol=? and volume> ? UNION ALL select  symbol,mktdate,volume  from "
				+ newbhav + "  WHERE mktdate<=? and symbol=? and volume> ?) t";
		PreparedStatement selQuery;
		ResultSet rs = null;
		int rank = 0;
		try {
			selQuery = connection.prepareStatement(volumePercentilQuery);
			selQuery.setString(1, mktdate);
			selQuery.setString(2, ticker.getSymbol());
			selQuery.setDouble(3, volume);
			selQuery.setString(4, mktdate);

			selQuery.setString(5, ticker.getSymbol());

			selQuery.setDouble(6, volume);
			rs = selQuery.executeQuery();
			while (rs.next()) {
				rank = rs.getInt("rank");
				System.err.println("Rank is " + ticker + " -- " + rank);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			rs.close();
			rs = null;
		}
		return rank;
	}

	boolean breakoutSimulation(String mktdate, Ticker ticker) throws SQLException, Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		double cutoffVolume = ticker.getBreakoutVolume() * surgeFactor;

		double expectedVolume = ticker.getLastVolume() * surgeFactor - 1;
		if (mktdate == null) {
			return false;
		}
		// System.err.println("PRocessing "+mktdate);
		java.util.Date date = format.parse(mktdate);
		SimpleDateFormat df = new SimpleDateFormat("yyyy");
		String year = df.format(date);
		String bhavName = "bhav" + year;
		String query = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma,prevclose,closeindictor,volatility,volume from "
				+ bhavName + " where symbol=? and mktdate=? order by mktdate asc";
		int oldyear = Integer.parseInt(year) - 1;
		String oldbhav = "bhav" + oldyear;
		PreparedStatement selQuery;
		ResultSet rs = null;
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker.getSymbol());
			selQuery.setString(2, mktdate);
			rs = selQuery.executeQuery();

			while (rs.next()) {

				double close = rs.getDouble("close");
				double open = rs.getDouble("open");
				double prevclose = rs.getDouble("prevclose");
				double high = rs.getDouble("high");
				double low = rs.getDouble("low");
				double volume = rs.getDouble("volume");
				double volatility = rs.getDouble("volatility");
				// if(checkAverageVolumes(ticker.getSymbol(),mktdate)){
				double idealBuyprice = ticker.getPivotBuyPrice() + ticker.getPivotBuyPrice() * pivotPlus / 100;
				double surge = volume / ticker.getBreakoutVolume();
				int rank = 0;

				ticker.setSurgeFactor(surge);
				if (idealBuyprice < high && (volume > cutoffVolume)) {

					// System.out.println(ticker.getSymbol() +" Has broken out
					// above "+ticker.getPivotBuyPrice()+" High is "+high+" On
					// volume "+cutoffVolume);
					if (open > ticker.getPivotBuyPrice()) {
						double gapup = 100 * ((open - prevclose) / prevclose);
						System.err.println(ticker + " Gapped up " + gapup + " on " + mktdate);
						if (gapup > Constants.gapupThreshold) {
							gapperCount++;
							return false;

						} else if (gapup < Constants.gapupThreshold) {
							ticker.setGappedup(true);
							double gapbuyprice = open + open * 1 / 100;
							if (gapbuyprice < high) {
								ticker.setBuyPrice(gapbuyprice);
							} else {
								ticker.setBuyPrice(high);
							}
							System.err.println(
									ticker.getSymbol() + " Gap up less than 4 take the trade" + ticker.getBuyPrice());

						}
					}

					return true;
				}
				// }
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			rs.close();
		}
		return false;
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

	private double getMaxVolume(String ticker, String mktdate, String bhav) {
		double maxvolume = 0;
		String query = "select max(volume) as maxvolume from " + bhav + " where symbol=? and  mktdate<?";
		PreparedStatement selQuery;
		boolean exited = false;

		// System.out.println("Gettng details for "+ticker.getSymbol()+" Bough
		// on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker);
			selQuery.setString(2, mktdate);
			// selQuery.setString(3, mktdate);
			ResultSet rs = selQuery.executeQuery();
			while (rs.next()) {
				maxvolume = rs.getDouble("maxvolume");

			}
			System.err.println("MAx Volume is " + maxvolume + " for " + ticker + " from mktdate " + mktdate);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return maxvolume;

	}

	private double getAverageVolatility(String ticker, String mktdate, String bhav) {
		double avgvolatility = 0;
		String query = "select avg(volatility) as avgvolatility from " + bhav
				+ " where symbol=? and  mktdate<? and mktdate>?-interval 30 DAY";
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
			// System.err.println("Avg Volatility is "+avgvolatility+" for
			// "+ticker+" from mktdate "+mktdate);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return avgvolatility;

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
		String query = "select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma,10dma,prevclose,closeindictor,volatility from "
				+ bhavName + " where symbol=? and mktdate=? order by mktdate asc";
		// System.err.println("Query "+query);
		PreparedStatement selQuery;
		boolean exited = false;

		try {
			selQuery = connection.prepareStatement(query);
			selQuery.setString(1, ticker.getSymbol());
			selQuery.setString(2, mktdate);
			ResultSet rs = selQuery.executeQuery();

			double buyPrice = ticker.getBuyPrice();
			double stoploss = ticker.getStopLoss();

			double exitPrice = 0;
			double scalpPrice = 0;
			int count = 0;
			int helddays = 0;
			boolean strengthsell=false;

			while (rs.next()) {
				count++;
				String closeindictor = rs.getString("closeindictor");

				// ticker=new Ticker();
				helddays = ticker.getHeldDays() + 1;
				ticker.setHeldDays(helddays);
				ticker.setPositiveIndictor(closeindictor);

				double close = rs.getDouble("close");
				double open = rs.getDouble("open");

				double high = rs.getDouble("high");
				double low = rs.getDouble("low");
				double idealBuyprice = 0;
				double dma10 = rs.getDouble("10dma");
				ticker.setDma10(dma10);
				double dma50 = rs.getDouble("50dma");
				ticker.setDma50(dma50);
				double dma20 = rs.getDouble("20dma");
				ticker.setDma20(dma20);
				double volatility = rs.getDouble("volatility");
				ticker.setVolatility(volatility);
				if (ticker.getBuyDate() == null) {
					
					double bodayLow = rs.getDouble("low");
					double prevclose = rs.getDouble("prevclose");
					ticker.setPrevclose(prevclose);
					double bodayhigh = rs.getDouble("high");

					 //buyPrice = close;

					idealBuyprice = ticker.getPivotBuyPrice() + ticker.getPivotBuyPrice() * pivotPlus / 100;
					buyPrice = idealBuyprice;
					if (ticker.isGappedup()) {
						System.err.println(ticker.getSymbol() + " Gapped up " + ticker.getBuyPrice());
						buyPrice = ticker.getBuyPrice();
					}
				//	double difffromclose = 100 * ((close - buyPrice) / buyPrice);

					// ticker.setBuyPrice(idealBuyprice);
					ticker.setBuyPrice(buyPrice);
					ticker.setBodayLow(bodayLow);
					ticker.setBreakoutHigh(bodayhigh);
					ticker.setBuyDate(mktdate);

//					double difftolow = 100 * ((buyPrice - bodayLow) / bodayLow);
//
////					stoploss = bodayLow - bodayLow * 1.5 / 100;
////					if (difftolow > 5) {
////						System.err.println("Low too far " + ticker);
////						stoploss = buyPrice - buyPrice * 6 / 100;
////					}
					double stoplossFactor=ticker.getAvgVolatility()*2 ;
					//stoploss = buyPrice - buyPrice * ticker.getAvgVolatility()*2 / 100;
					stoploss = ticker.getLow10day() - ticker.getLow10day()  * 1.5 / 100;
//					if (stoplossFactor > 8) {
//						System.err.println("Too far stop loss too far " + ticker);
//						stoploss = buyPrice - buyPrice * 8 / 100;
//					}
					// stoploss=Math.floor(stoploss);
					scalpPrice = buyPrice + buyPrice * scalplevel / 100;
					double samedaystoploss = buyPrice - buyPrice * Constants.samedaystoplosslevel / 100;
					// System.err.println("First time "+stoploss);
					ticker.setStopLoss(stoploss);
					ticker.setScalpPrice(scalpPrice);
					// ticker.setScalpStoploss(scalpStop);
					getAllocation(ticker);
					availableCapacity = availableCapacity - 1;

					////
//					if (close < samedaystoploss) {
//						System.err.println(mktdate + " Same day exit for " + ticker.getSymbol() + " Bought at "
//								+ ticker.getBuyPrice() + " Has fallen " + difffromclose + " stoploss "
//								+ samedaystoploss);
//
//						ticker.setExitDate(mktdate);
//						exited = true;
//						ticker.setExitPrice(samedaystoploss);
//						ticker.setStoplossHit(true);
//
//						// trades.add(ticker);
//						break;
//
//					}
					//
				

					// System.out.println("BuyPrice : "+buyPrice+" Stoploss
					// "+stoploss);

				}
				//
			
				//System.err.println(mktdate + " Stop loss " + ticker + " is " + ticker.getStopLoss());

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
				//double trailstoploss = close - close * 8 / 100;
				// double trailstoploss = ticker.getStopLoss();
				if(close>ticker.getHighest()){
					ticker.setHighest(close);
					}
				ticker.setPassThreshold(true);
				double riseFrom50DMA=100 * ((high - dma50) / dma50);
				if(helddays > 1 && riseFrom50DMA>50){
					System.err.println(ticker+" Too far from 50dma !! exit");
					ticker.setExitDate(mktdate);
					exited = true;
					double soldPrice = close - close * 0.4 / 100;
					ticker.setExitPrice(soldPrice);
					ticker.setStoplossHit(true);
					break;
				}
				
	//			double riseFrom10DMA=100 * ((high - dma10) / dma10);
//				if(riseFrom10DMA>20){
//					System.err.println("Too far from 10dma !! exit");
//					ticker.setExitDate(mktdate);
//					exited = true;
//					double soldPrice = close - close * 0.4 / 100;
//					ticker.setExitPrice(soldPrice);
//					ticker.setStoplossHit(true);
//					break;
//				}
//				
				double riseFrom20DMA=100 * ((high - dma20) / dma20);
				if(helddays > 1 && riseFrom20DMA>50){
					System.err.println("Too far from 20dma !! exit");
					ticker.setExitDate(mktdate);
					exited = true;
					double soldPrice = close - close * 0.4 / 100;
					ticker.setExitPrice(soldPrice);
					ticker.setStoplossHit(true);
					break;
				}
				

//				 if (trailstoploss > stoploss) {
//				 //System.err.println(ticker);
//				 trailstoploss=Math.floor(trailstoploss*100.0)/100.0;
//				 ticker.setStopLoss(trailstoploss);
//				 // System.err.println("Old Stop Loss "+stoploss+" New
//				 // Stop loss "+trailstoploss+ " Close "+close);
//				 }

				//
			
				
				if (helddays > 1 && open < ticker.getStopLoss()) {
					System.err.println(ticker + " Open below Stoploss is " + ticker.getStopLoss());
					ticker.setExitDate(mktdate);
					exited = true;
					double soldPrice = open - open * 0.4 / 100;
					ticker.setExitPrice(soldPrice);
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
				if (helddays > 1 && low < ticker.getStopLoss()) {
					System.err.println(ticker + " Hit Stoploss is " + ticker.getStopLoss());
					ticker.setExitDate(mktdate);
					exited = true;
					double soldPrice = ticker.getStopLoss() - ticker.getStopLoss() * 0.2 / 100;
					//double soldPrice = close * 0.2 / 100;
					ticker.setExitPrice(soldPrice);
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
				
				if (helddays > 1 && close < ticker.getTrailer()) {
					System.err.println(ticker + " Trailer Stoploss is " + ticker.getTrailer());
					ticker.setExitDate(mktdate);
					exited = true;
					double soldPrice = close - close * 0.2 / 100;
					//double soldPrice = close * 0.2 / 100;
					ticker.setExitPrice(soldPrice);
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
//				if (helddays > 20 && percentdiff < 5) {
//				//System.err.println(ticker+" 20 day Falling more than 5 percent "+percentdiff);
//				ticker.setExitTommorow(true);
//				ticker.setSquatted(true);
//				System.err.println("Cutting loser " + ticker.getSymbol() + " Squatted!");
//				break;
//			}
				// if (helddays>1 && close < ticker.getDma10()) {
				// System.err.println(ticker+" Stoploss is
				// "+ticker.getStopLoss());
				// ticker.setExitDate(mktdate);
				// exited = true;
				// double soldPrice=close- close*0.2/100;
				// ticker.setExitPrice(soldPrice);
				// ticker.setStoplossHit(true);
				//
				// // trades.add(ticker);
				// break;
				//
				// }
				if (helddays > 1 && close < ticker.getDma10()) {
					
					ticker.setExitDate(mktdate);
					double newsl=close- close *1 / 100;
					if(newsl>ticker.getStopLoss())
					ticker.setTrailer(newsl);
					else
						ticker.setTrailer(ticker.getStopLoss());
					System.out.println(ticker + " below  10dma reached " + mktdate + " close " + close + " stoploss "
							+ ticker.getStopLoss());
//					exited = true;
//					double soldPrice = close - close * 0.2 / 100;
//
//					ticker.setExitPrice(soldPrice);
//					ticker.setStoplossHit(true);

					// trades.add(ticker);

				}
				
				double percentdiff = 100 * ((close - buyPrice) / buyPrice);
//				System.err.println("Percent diff "+percentdiff);
//				if (percentdiff >trailstoplossLevel && !strengthsell) {
//					double trailstoploss = close - close * trailstoplossLevel / 100;
//				
//					
//					ticker.setPassThreshold(true);
//
//					ticker.setStopLoss(ticker.getBuyPrice()+ticker.getBuyPrice()*1/100);
//					
//					if (trailstoploss > stoploss) {
//
//						//System.err.println(ticker);
//						//trailstoploss=Math.round(trailstoploss);
//						//ticker.setStopLoss(trailstoploss);
//						//ticker.setStopLoss(stopLoss);
//						ticker.setTrailer(trailstoploss);
//						System.err.println("Setting trailer to protect profits "+ticker+" Trailer "+ticker.getTrailer());
//
//						// System.err.println("Old Stop Loss "+stoploss+" New
//						// Stop loss "+trailstoploss+ " Close "+close);
//					}
//					// System.out.println("Close is : "+close+"New Stop Loss is
//					// "+stoploss);
//				}
				
//				if (percentdiff >trailstoplossLevel*1.5) {
//
//					ticker.setStopLoss(ticker.getBuyPrice()-ticker.getBuyPrice()*2/100);
//					System.err.println("Upgrading stop to protect profits "+ticker+" stoploss "+ticker.getStopLoss());
//
//
//				}
				double strengthsl=0;
				if (helddays > 2 && volatility > ticker.getAvgVolatility()*1.75) {
					System.err.println(mktdate+" "+ticker + " Volatility increased is " + volatility);
					 strengthsl=close-close*4/100;
					ticker.setStopLoss(strengthsl);
					System.err.println(ticker + " Volatility increased strength sl is " + strengthsl);
					strengthsell=true;
					ticker.setStrengthsell(strengthsell);

				}
				
				if (helddays > 2 && percentdiff > ticker.getAvgVolatility()*2) {
					 strengthsl=buyPrice+buyPrice*2/100;
					 System.err.println(mktdate+" Close is "+close+" sl "+strengthsl);
					 if(strengthsl>ticker.getStopLoss()){
					ticker.setStopLoss(strengthsl);
					System.err.println(ticker +" "+ close+" Locking profits increased strength sl is " + strengthsl+" "+ticker.getAvgVolatility());
					 }
//					strengthsell=true;
//					ticker.setStrengthsell(strengthsell);

				}
				
//				if(ticker.isStrengthsell()){
//
//					double newstrengthsl=close-close*3/100;
//					if(newstrengthsl>strengthsl)
//					ticker.setStopLoss(newstrengthsl);
//					System.err.println(ticker + " Updating increased strength sl is " + ticker.getStopLoss());
//
//
//				}
				
				if (helddays > 20 && percentdiff < 3) {
					ticker.setExitDate(mktdate);
					exited = true;
					//ticker.setExitPrice(ticker.getStopLoss());
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
				
				
				if (helddays > 30 && percentdiff < 10) {
					ticker.setExitDate(mktdate);
					exited = true;
					//ticker.setExitPrice(ticker.getStopLoss());
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
				
				if (helddays > 40 && percentdiff < 15) {
					ticker.setExitDate(mktdate);
					exited = true;
					//ticker.setExitPrice(ticker.getStopLoss());
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}
				
				if (helddays > 60 && percentdiff < 30) {
					ticker.setExitDate(mktdate);
					exited = true;
					//ticker.setExitPrice(ticker.getStopLoss());
					ticker.setStoplossHit(true);

					// trades.add(ticker);
					break;

				}

				ticker.setExitDate(mktdate);

			}

			if (count == 0) {
				System.err.println(ticker + " is empty exiting " + ticker.getExitDate());
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
			prep.setDouble(3, trade.getBuyQty());
			prep.setDouble(4, trade.getBuyPrice());
			prep.setDouble(5, Math.round(trade.getAllocatedCapital()));

			prep.setDouble(6, trade.getExitPrice());
			prep.setString(7, trade.getExitDate());
			prep.setString(8, trade.isStoplossHit() ? "Y" : "N");
			prep.setDouble(9, trade.getStopLoss());
			prep.setString(10, trade.isSquatted() ? "Y" : "N");
			prep.setInt(11, trade.getHeldDays());
			prep.setString(12, trade.isNeilWinner() ? "Y" : "N");
			prep.setString(13, trade.isExitTommorow() ? "Y" : "N");
			prep.setDouble(14, trade.getRoc());
			prep.setDouble(15, trade.getRoc63());
			prep.setDouble(16, trade.getPrevclose());
			prep.setDouble(17, trade.getHighest());
			prep.setDouble(18, trade.getScalpPrice());
			prep.setString(19, trade.isScalped() ? "Y" : "N");
			prep.setDouble(20, trade.getScalpProfit());
			prep.setDouble(21, trade.getBreakoutVolume());
			prep.setDouble(22, trade.getPivotBuyPrice());
			prep.setDouble(23, trade.getSurgeFactor());

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
			String mktdate = capital.getMktdate();
			if (mktdate != null) {
				prep.setString(1, mktdate);
				prep.setDouble(2, capital.getInvestedCapital());
				prep.setDouble(3, capital.getAvailableCapital());
				prep.addBatch();
			}

		}
		prep.executeBatch();
		connection.commit();
	}

	private void getAllocation(Ticker trade) {
		if (!firstAllocationDone) {
			initialAllocationCounter++;
		}
		double capitalToUse = 0;
		double tradeCapital = 0;
		if (!firstAllocationDone) {
			capitalToUse = initialCapital;
			tradeCapital = capitalToUse / maxcapacity;

		} else {
			capitalToUse = totalCapital;
			tradeCapital = capitalToUse / availableCapacity;
		}

		totalCapital = totalCapital - tradeCapital;
		double boughtQuantity = tradeCapital / trade.getBuyPrice();

		trade.setBuyQty(boughtQuantity);
		trade.setAllocatedCapital(tradeCapital);
		if (initialAllocationCounter == TOTALHOLDINGS) {
			firstAllocationDone = true;
		}

	}

	private ArrayList<DailyCapitalStatus> capitalList = new ArrayList<DailyCapitalStatus>();
	public static final String eodcapital = "insert into cancapitalstatus values(?,?,?)";

	private void calculateEOD(String mktdate) {
		DailyCapitalStatus obj = new DailyCapitalStatus();
		double capitalinplay = 0;
		double originalcost = 0;
		for (Ticker trade : this.trades) {
			double invested = trade.getBuyPrice() * trade.getBuyQty();
			double currentvalue = trade.getExitPrice() * trade.getBuyQty();
			// System.err.println(trade+" "+invested);
			// System.err.println(trade+" "+currentvalue);

			capitalinplay = capitalinplay + currentvalue;
			originalcost = originalcost + invested;

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
		// totalCapital=100000;o
		// System.err.println("Capital "+totalCapital);
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		String date = simpleDateFormat.format(new java.util.Date());
		System.out.println(date);

		ivfLowSLSimulator obj = new ivfLowSLSimulator();
		String startdate = "2020-01-01";
		String enddate = "2017-12-31";
		enddate = date;
		obj.connect();
		// obj.doVCP();
		obj.loadMarketDates(startdate, enddate);

		int count = maxcapacity;

		double totalearned = 0;
		int holdingperiod = 0;
		// double allocation=capital/8;
		// obj.currentDay=new
		// java.util.Date(formatter.parse(buydate).getTime());

		System.err.println("Market dates " + obj.mktdates.size());

		for (int i = 0; i < obj.mktdates.size(); i++) {

			String today = obj.mktdates.get(i);
			String nextday = obj.getNextDay(today);
			// obj.loadBuyAlerts(today, count);
			obj.loadCandidates(today);

			// System.err.println("trading on "+today);
			Iterator<Ticker> iterator = obj.trades.iterator();
			while (iterator.hasNext()) {
				Ticker ticker = iterator.next();

				boolean exited = obj.simulateTrade(ticker, nextday);
				if (exited) {
					double tradeProceeds = ticker.getExitPrice() * ticker.getBuyQty();

					totalCapital = totalCapital + tradeProceeds;

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
		System.err.println("AvailableCapacity " + availableCapacity);
		System.err.println("Available Capital " + totalCapital);
		System.err.println("Gapper " + obj.gapperCount);

		// System.err.println("Ideal buy point hit count
		// "+obj.idealBuypointhit);

		for (Ticker trade : obj.trades) {
			double invested = trade.getExitPrice() * trade.getBuyQty();
			investedCapital = investedCapital + invested;

		}

		System.err.format("%f%n", investedCapital);
		System.err.format("%f%n", totalCapital);
		System.err.format("Total Capital " + "%f%n", investedCapital + totalCapital);

		System.err.println("Scalped Capital " + scalpedCapital);

		System.err.format("%f%n", scalpedCapital);

		for (Ticker trade : obj.alltrades) {
			holdingperiod = holdingperiod + trade.getHeldDays();
			double exitprice = trade.getExitPrice();
			double buyprice = trade.getBuyPrice();
			double Bought = trade.getBuyQty();
			double investedamount = Bought * buyprice;
			double Sold = exitprice * Bought;
			double gainloss = Sold - (Bought * buyprice);

			gainloss = Math.round(gainloss * 100.0) / 100.0;
			double Result = investedamount + gainloss;

			if (exitprice >= buyprice) {
				winners++;
				double gain = 100 * ((exitprice - buyprice) / buyprice);
				double profit = exitprice - buyprice;

				totalwin = totalwin + gain;
				// System.out.println(trade.getSymbol() + " Bought on " +
				// trade.getBuyPrice() + " on " + trade.getBuyDate()
				// + " Exit Price " + trade.getExitPrice() + " on " +
				// trade.getExitDate() + " Gain :" + gainloss
				// + " Held :" + trade.getHeldDays() + " Days" + " Invested:" +
				// investedamount + " Stop loss: "
				// + trade.getStopLoss());
			} else {
				losers++;
				double loss = 100 * ((exitprice - buyprice) / buyprice);
				double lost = buyprice - exitprice;
				// capital=capital-lost;
				// System.err.println(trade.getSymbol() + " Bought on " +
				// trade.getBuyPrice() + " on " + trade.getBuyDate()
				// + " Exit Price " + trade.getExitPrice() + " on " +
				// trade.getExitDate() + " Lost :" + gainloss
				// + " Held :" + trade.getHeldDays() + " Days" + " Stop loss: "
				// + trade.getStopLoss());
				// if (loss < -10) {
				// System.err.println(trade + " Lost " + loss);
				// }
				totalloss = totalloss + loss;

			}
			totalearned = totalearned + Result;
		}
		obj.insertTradeHistory();
		obj.insertEODStatus();
		double totaltrades = winners + losers;

		double perwin = winners / totaltrades;
		perwin *= 100;

		System.err.println("Winners " + winners + " Losers " + losers + " total " + totaltrades);
		System.err.println("Win % " + perwin + " Loss " + (losers / totaltrades) * 100);
		// System.err.println("Avg Holding period :" + holdingperiod /
		// obj.completedtrades.size() + " Days");

		// System.err.println("Win % " + totalwin + " Lost% " + totalloss);
		System.err.println("Scalp % " + (obj.scalpCount / totaltrades) * 100);

		System.err.println("Average Win % " + totalwin / winners + " Lost% " + totalloss / losers);

		System.err.println("Total trades added :" + obj.tradecounter);
		System.err.println("Completed trades  :" + obj.completedtrades.size());
		System.err.println("Total Scalps  :" + obj.scalpCount);

		System.err.println("Open trades  :" + obj.trades.size());
		System.err.println("Squatters  :" + obj.squatters);
		System.err.println("Open Trade  :" + obj.trades);
		////
		//// System.err.println("Neil winners :" + obj.neilwinners.size());
		//// for (Ticker trade : obj.neilwinners) {
		//// System.err.println(
		//// trade + " " + trade.getBuyPrice() + " " + trade.getExitPrice() + "
		//// Gain " + trade.getPercentGain());
		//// }

	}

}
