package bhav;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
public class TmlSystem {
	Connection connection = null;
	int fileCounter=0;
	HashSet<String> holidays=new HashSet();
	private ArrayList<Ticker> buyList=null;
	
	private Date startDate;
	private Date endDate;
	private String monthToChase;
	private java.util.Date currentDay;
	ArrayList<String> mktdates = null;

	private String startDateString=null;
	private String endDateString=null;
	private String performanceDateString=null;
	private String runDateString=null;
	private String restartDateString=null;
	private String reendDateString=null;
	private Date rebalstartDate;
	private Date rebalendDate;
	private Date rebRunDate;
	
	private String strLastday=null;
	
	private String rebRunDateString=null;

	public static int stopLoss=20;
	
	private boolean threeMonthRun=false;
	
	
	private int bhavYear;
	
	private void loadMomentumBuyList() throws SQLException{
		
		String fromTableName=null;
		String toTableName=null;
		
		
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(startDate);

		int startYear=cal.get(Calendar.YEAR);
		cal.setTime(endDate);
		int endYear=cal.get(Calendar.YEAR);
		fromTableName="bhav"+endYear;
		toTableName="bhav"+startYear;
		CallableStatement stmt=null;
		try{
//			if(monthToChase.startsWith("FEB") || monthToChase.startsWith("MAR")){
//				toTableName="bhav"+(startYear+1);
//			}
		System.out.println("call fetchtmlbuyList('"+startDateString+"','"+fromTableName+"','"+toTableName+"','"+monthToChase+"')");
		
//		System.out.println("call fetchMomentumBuyList('"+startDateString+"','"+endDateString+"','"+fromTableName+"','"+toTableName+"','"+monthToChase+"','"+runDateString+"',@curroc,@avgroc1)");

		//String buylistquery="call fetchMomentumBuyList(?,?,?,?,?,?,?,?)";
		
		String buylistquery="call fetchtmlbuyList(?,?,?,?)";
		stmt=connection.prepareCall(buylistquery);
		
		stmt.setString(1, startDateString);
		stmt.setString(2, fromTableName);
		stmt.setString(3, toTableName);

			stmt.setString(4, monthToChase);	
		
		stmt.execute();
		
		
		
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}finally {
			stmt.close();
			
			
		}

	}
	private void closeconnection() throws SQLException{
		connection.close();
	}
	
	private void filterForMomentQuality(){
		
	}
	
	
	public java.util.Date getReStartDate(java.util.Date date) {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    cal.add(Calendar.MONTH, -6);
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
	        cal.add(Calendar.DAY_OF_MONTH, -1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

	    }
	    
	    return cal.getTime();
	}

	
	private void setUpDates(java.util.Date currentDay) throws Exception{
		//get 11 month range
		
  	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
//
//  	  java.util.Date currentDay=new java.util.Date(formatter.parse(month).getTime());  
	  SimpleDateFormat formatte1r=new SimpleDateFormat("MMMM");
	  SimpleDateFormat formatter2=new SimpleDateFormat("YYYY");
  	  //java.util.Date currentMonth=new java.util.Date(formatte1r.parse(month).getTime());
 	  
  	
    this.monthToChase=formatte1r.format(currentDay).toUpperCase()+formatter2.format(currentDay).toUpperCase();
  	  System.out.println(monthToChase);
  	  //this.monthToChase=currentMonth.toString();
  	  
	    startDate=getStartDate(currentDay);
	    
	  	  System.out.println(startDate);

    endDate=getEndDate(currentDay);
	    startDateString=formatter.format(startDate);
	    endDateString=formatter.format(endDate);
	    runDateString=formatter.format(currentDay);
	    
	    runDateString=formatter.format(currentDay);

//
	    Calendar cal1=Calendar.getInstance();
	    cal1.setTime(currentDay);
//	    
//	    //last day of month
	    Calendar lastday=Calendar.getInstance();
	    lastday.setTime(currentDay);

	    lastday.set(Calendar.DATE, lastday.getActualMaximum(Calendar.DATE));
//
	    Date lastDayOfMonth = lastday.getTime();
	     strLastday=formatter.format(lastDayOfMonth);
		bhavYear=cal1.get(Calendar.YEAR);
//		
//	    
//	    
//	    cal1.add(Calendar.DAY_OF_MONTH, 10);
//	    rebalstartDate=getReStartDate(cal1.getTime());
//	    rebalendDate=getEndDate(cal1.getTime());
//	    restartDateString=formatter.format(rebalstartDate);
//	    reendDateString=formatter.format(rebalendDate);
//	  
//	    rebRunDate=getRunDate(cal1.getTime());
//	    
//	    rebRunDateString=formatter.format(rebRunDate);
//
//		//date for performance stats
//	    
//	    getFirstWorkingDayOfMonth(currentDay);
	    performanceDateString=formatter.format(getFirstWorkingDayOfMonth(currentDay));
	    
	}
	
	public java.util.Date getRunDate(java.util.Date date) {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
	        cal.add(Calendar.DAY_OF_MONTH, -1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

	    }
	    return cal.getTime();
	}

	private void connect(){
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection =
		       DriverManager.getConnection("jdbc:mysql://localhost/bhav?" +
		                                   "user=root&password=root");
		    
		    System.out.println("Connected !");
		} catch (Exception ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    ex.printStackTrace();
		    //System.out.println("SQLState: " + ex.getSQLState());
		    //System.out.println("VendorError: " + ex.getErrorCode());
		}
		
	}

	private void loadHolidays() throws SQLException{
		
		String query="select Date from holidaylist";
		
		Statement selQuery;
		try {
			selQuery = connection.createStatement();
			ResultSet rs=selQuery.executeQuery(query);
			while(rs.next()){
				holidays.add(rs.getString("Date"));
			}
			System.out.println("loaded holidays "+holidays.size());
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}finally{
			
		}
	}

	public java.util.Date getStartDate(java.util.Date date) {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
//	    cal.add(Calendar.MONTH, -6);
//	    
//	    int dayOfWeek;
//	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
//	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
//	        cal.add(Calendar.DAY_OF_MONTH, 1);
//	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
//
//	    }
	    return cal.getTime();
	}

	public java.util.Date getFirstWorkingDayOfMonth(java.util.Date date) {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    
	    
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
	        cal.add(Calendar.DAY_OF_MONTH, 1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

	    }
	    return cal.getTime();
	}

	
	public java.util.Date getEndDate(java.util.Date date) {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    cal.add(Calendar.MONTH, -12);
	    
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
//	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
//	        cal.add(Calendar.DAY_OF_MONTH, -1);
//	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
//
//	    }
	    return cal.getTime();
	}

	
	private boolean isPrevDayHoliday(java.util.Date date){
		boolean isHoliday=false;
		
		DateFormat destDf = new SimpleDateFormat("dd-MMM-yy");
		            // format the date into another format

		           String dateStr = destDf.format(date);

		             

		
		isHoliday=holidays.contains(dateStr);
		return isHoliday;
		
	}
	
	private double calculatePortfolioEOD(String portfolio) throws SQLException{
		loadMarketDates(runDateString,strLastday);
		double returns=0;
		for(String today:mktdates){
			//System.err.println("REturns on "+today+" are : "+returns);
			String portfoliostatus="select sum(InvestedAmount),sum(realizedamount),((Realizedamount-InvestedAmount)/InvestedAmount)*100 as returns from portfolio."+portfolio+"portfolio";
			Statement st=connection.createStatement();
			ResultSet rs=st.executeQuery(portfoliostatus);
			
			while(rs.next()){
				returns=rs.getDouble("returns");
			//	System.err.println("Returns on "+today+" are : "+returns);				
				if(returns<=-15){
				//	System.err.println("Exit Portfolio tomorrow !! "+today);
				}
			}
		}
		
		
	
	
		return returns;

	}
	
	private void updatePortfolio(String portfolio,Ticker t) throws SQLException{
		String updatequery="update portfolio."+portfolio+"portfolio set sellprice=? where symbol=?";
		
		PreparedStatement st=connection.prepareStatement(updatequery);
		
		st.setDouble(1, t.getExitPrice());
		st.setString(2, t.getSymbol());
		st.executeUpdate();
	}
	private void simulateTrades() throws SQLException{
		//load the portfolio for the month
		String loadPortfolio="SELECT symbol,BuyPrice,BuyQty from portfolio."+monthToChase+"portfolio;";
		//Load dates for the month
		loadMarketDates(runDateString,strLastday);
		
		String query = "select symbol,mktdate,open,close,high,low from bhav" + bhavYear + " where symbol=? and mktdate=?";
				Statement st=connection.createStatement();
		ResultSet rs=st.executeQuery(loadPortfolio);
		buyList=new ArrayList<Ticker>();
		while(rs.next()){
			Ticker ticker=new Ticker();
			ticker.setSymbol(rs.getString("symbol"));
			double buyPrice=rs.getDouble("BuyPrice");
			ticker.setBuyPrice(buyPrice);
			double stoploss = buyPrice - buyPrice * stopLoss / 100;
			ticker.setStopLoss(stoploss);
			buyList.add(ticker);
		}
		 
		double InvestedAmount=0;
		double currentvalue=0;
		
		for(Ticker t:buyList){
			for(String today:mktdates){
			PreparedStatement ps=connection.prepareStatement(query);
			ps.setString(1, t.getSymbol());
			ps.setString(2, today);
			
			ResultSet rs1=ps.executeQuery();
			while(rs1.next()){
				double close=rs1.getDouble("close");
				if(close<t.getStopLoss()){
					//
					System.err.println(t+" Stoploss hit");
					t.setStoplossHit(true);
					t.setExitPrice(close);
					t.setExitDate(today);
					
					//update exit in portfolio
					
					updatePortfolio(monthToChase,t);
					break;
					
				}
			}
			if(t.isStoplossHit()) break;
			
			}
		}
 
		
		
		//run through the portfolio
	}
	
	
	private double calculatePortfolioDrawdown() throws SQLException{
		//load the portfolio for the month
		String tomorrow=null;
		String loadPortfolio="SELECT symbol,BuyPrice,BuyQty,InvestedAmount,Realizedamount from portfolio."+monthToChase+"portfolio;";
		//Load dates for the month
		loadMarketDates(runDateString,strLastday);
		double drawdown=0;
		boolean circuithit=false;
		String query = "select symbol,mktdate,open,close,high,low from bhav" + bhavYear + " where symbol=? and mktdate=?";
				Statement st=connection.createStatement();
		ResultSet rs=st.executeQuery(loadPortfolio);
		buyList=new ArrayList<Ticker>();
		double InvestedAmount=0;
		double portfolioPeak=0;
		while(rs.next()){
			Ticker ticker=new Ticker();
			ticker.setSymbol(rs.getString("symbol"));
			double buyPrice=rs.getDouble("BuyPrice");
			double buyQty=rs.getDouble("BuyQty");
			double invested=rs.getDouble("InvestedAmount");
			double realized=rs.getDouble("Realizedamount");
			InvestedAmount=InvestedAmount+invested;
			ticker.setBuyPrice(buyPrice);
			ticker.setBuyQty(buyQty);
			ticker.setAllocatedCapital(invested);
			if(ticker.getSymbol().equalsIgnoreCase("Liquid")){
				ticker.setAllocatedCapital(realized);
			}
			buyList.add(ticker);
		}
	

	System.err.format("Total Invested Amount is : "+"%f%n",InvestedAmount);

		double currentvalue=0;
		 String dayProcessed=null;
		Iterator<String> iterator=mktdates.iterator();
		 
		while(iterator.hasNext()){
			String today=iterator.next();
			
				 currentvalue=0;
				for(Ticker t:buyList){
				dayProcessed=today;
				
			PreparedStatement ps=connection.prepareStatement(query);
			ps.setString(1, t.getSymbol());
			ps.setString(2, today);
			
			ResultSet rs1=ps.executeQuery();
			while(rs1.next()){
				double close=rs1.getDouble("close");
				double currentPrice=close*t.getBuyQty();
				currentvalue=currentvalue+currentPrice;
				
			}
			if(t.getSymbol().equalsIgnoreCase("Liquid")){
				currentvalue=currentvalue+t.getAllocatedCapital();
			}
		
			
			}
				//System.err.format(dayProcessed+" InvestedAmount : %f%n",InvestedAmount);
				
			//	System.err.format(" current value: %f%n",currentvalue);
				
			portfolioPeak=getPeakPortfolio();
			
			//System.err.format(" Portfolio Peak: %f%n",portfolioPeak);

				//double returns=((currentvalue-InvestedAmount)/InvestedAmount)*100;
				
				 drawdown=((currentvalue-portfolioPeak)/portfolioPeak)*100;
				 
					//System.err.println("Drawdown on "+today +" exceeded"+drawdown);

				//	System.err.format("Peak Amount is : "+"%f%n",portfolioPeak);
				insertEODStatus(monthToChase,today,InvestedAmount,currentvalue,portfolioPeak,drawdown);
				if(drawdown<-20){
				System.err.println("Drawdown on "+today +" exceeded"+drawdown);
				
				createPerformanceStatsTillDate(today);
				
				return drawdown;
				}



		
		}
		createPerformanceStats();
			return drawdown;
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
	
	private void createPerformanceStatsTillDate(String enddate) throws SQLException{
		try{
		//	simulateTrades();
		CallableStatement stmt;
		String buylistquery=null;
		if(threeMonthRun){
		buylistquery="call calculatePerformanceTillDate(?,?,?)";
		System.out.println("call calculatePerformanceTillDate('"+performanceDateString+"','"+enddate+"','"+monthToChase+"')");
				}
		else{
			buylistquery="call calculatePerformanceTillDate(?,?,?)";
			System.out.println("call calculatePerformanceTillDate('"+performanceDateString+"','"+enddate+"','"+monthToChase+"')");	
		}
		
		stmt=connection.prepareCall(buylistquery);
		
		stmt.setString(1, performanceDateString);
		stmt.setString(2, enddate);
		stmt.setString(3, monthToChase);
	
		//stmt.registerOutParameter(3, java.sql.Types.DOUBLE);
		stmt.execute();
		calculatePortfolioEOD(monthToChase);
		//System.err.println("Allocation for stocks -->"+stmt.getDouble(3)+"%");
		System.err.println("Completed");
		//connection.commit();
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		
		
	}
	
	private void createPerformanceStats() throws SQLException{
		try{
		//	simulateTrades();
		CallableStatement stmt;
		String buylistquery=null;
		if(threeMonthRun){
		buylistquery="call calculatePerformance(?,?)";
		System.out.println("call calculatePerformance('"+performanceDateString+"',"+monthToChase+"')");
		}
		else{
			buylistquery="call calculatePerformance(?,?)";
			System.out.println("call calculatePerformance('"+performanceDateString+",'"+monthToChase+"')");
		}
		
		stmt=connection.prepareCall(buylistquery);
		
		stmt.setString(1, performanceDateString);
		stmt.setString(2, monthToChase);
		//stmt.registerOutParameter(3, java.sql.Types.DOUBLE);
		stmt.execute();
		calculatePortfolioEOD(monthToChase);
		//System.err.println("Allocation for stocks -->"+stmt.getDouble(3)+"%");
		System.err.println("Completed");
		//connection.commit();
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		
		
	}
	
	
	
	private double getPeakPortfolio() throws SQLException {
		
		//check if circuit hit previous month
		
		String sql1="select max(circuitdate) as latestcircuit from portfolio.corpus;";
		String circuithit=null;
		String circuithitdate=null;
		PreparedStatement prep = connection.prepareStatement(sql1);
		double portfoliopeak=0;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			circuithitdate=rs.getString("latestcircuit");	

			
		}
		
		StringBuilder sbSql = new StringBuilder( 1024 );
		
		sbSql.append("select max(currentvalue) as portfoliopeak from portfolio.portfoliostatus");
		if(circuithitdate!=null){
			sbSql.append(" where mktdate>=?");
		}
		 prep = connection.prepareStatement(sbSql.toString());

		if(circuithitdate!=null){
			prep.setString(1,circuithitdate);

		}
		 
		 
		 rs = prep.executeQuery();
		while (rs.next()) {
			portfoliopeak = rs.getDouble(1);
			
			;
		}
		return portfoliopeak;
	}

	
	private void rebalancePortfolio() throws Exception{
		String fromTableName=null;
		String toTableName=null;
		
		
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(startDate);

		int startYear=cal.get(Calendar.YEAR);
		cal.setTime(endDate);
		int endYear=cal.get(Calendar.YEAR);
		fromTableName="bhav"+startYear;
		toTableName="bhav"+endYear;
		CallableStatement stmt=null;
		try{
			if(monthToChase.startsWith("JAN")|| monthToChase.startsWith("FEB") || monthToChase.startsWith("MAR")){
				toTableName="bhav"+(startYear+1);
			}
				System.out.println("call portfoliohealthcheck('"+restartDateString+"','"+reendDateString+"','"+fromTableName+"','"+toTableName+"','"+monthToChase+"','"+rebRunDateString+"')");
			
		String buylistquery="call portfoliohealthcheck(?,?,?,?,?,?)";

		stmt=connection.prepareCall(buylistquery);
		
		stmt.setString(1, restartDateString);
		stmt.setString(2, reendDateString);
		stmt.setString(3, fromTableName);
		stmt.setString(4, toTableName);
		stmt.setString(5, monthToChase);	
		stmt.setString(6, rebRunDateString);
		stmt.execute();
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}finally {
			stmt.close();
			
			
		}

	}

	private void loopTrendTemplate() throws Exception{
		String fromTableName=null;
		String toTableName=null;
		
		
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(startDate);

		int startYear=cal.get(Calendar.YEAR);
		cal.setTime(endDate);
		int endYear=cal.get(Calendar.YEAR);
		fromTableName="bhav"+startYear;
		toTableName="bhav"+endYear;
		CallableStatement stmt=null;
		try{
			if(monthToChase.startsWith("JAN")|| monthToChase.startsWith("FEB") || monthToChase.startsWith("MAR")){
				toTableName="bhav"+(startYear+1);
			}
				System.out.println("call looptrendtemplate('"+restartDateString+"','"+reendDateString);
			
		String buylistquery="call looptrendtemplate(?,?)";

		stmt=connection.prepareCall(buylistquery);
		
		stmt.setString(1, restartDateString);
		stmt.setString(2, reendDateString);
		stmt.execute();
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}finally {
			stmt.close();
			
			
		}

	}
	public static final String eodcapital="insert into portfolio.portfoliostatus(month,mktdate,investedamount,currentvalue,portfoliopeak,returns) values(?,?,?,?,?,?)";
	private void insertEODStatus(String month,String mktdate,double investedamount,double realizedamount,double portfoliopeak,double returns) throws SQLException {
		//connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		//st.execute("truncate table portfoliostatus");
		PreparedStatement prep = connection.prepareStatement(eodcapital);

		
		
			prep.setString(1, month);
			prep.setString(2, mktdate);
			prep.setDouble(3, investedamount);
			prep.setDouble(4, realizedamount);
			prep.setDouble(5, portfoliopeak);
			prep.setDouble(6, 0);
			prep.execute();
	

	
//		prep.executeBatch();
//		connection.commit();
	}
	
	private void cleanUp(){
		
	}
	
	public static void main(String[] args)throws Exception{
		TmlSystem mom=new TmlSystem();
		mom.connect();
		mom.loadHolidays();
		String start="2007-01-01";
			
		boolean generatePerformanceStats=true;	
		mom.threeMonthRun=false;
	  	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");

	  	  mom.currentDay=new java.util.Date(formatter.parse(start).getTime());  
Calendar cal=Calendar.getInstance();
cal.setTime(mom.currentDay);
java.util.Date monthoprocess;
		for (int i=0;i<200;i++){
			 monthoprocess=cal.getTime();
			 System.err.println("Processing "+monthoprocess);
			mom.setUpDates(monthoprocess);
			mom.loadMomentumBuyList();
			//mom.rebalancePortfolio();

			if(generatePerformanceStats) {
					mom.createPerformanceStats();
		//	double drawdown=mom.calculatePortfolioDrawdown();
				
			}
				
	cal.add(Calendar.MONTH, 1);
	}
		mom.closeconnection();
		
		
		
		
	}
}
