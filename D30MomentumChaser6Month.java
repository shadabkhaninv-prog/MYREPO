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
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import javax.swing.text.DateFormatter;
public class D30MomentumChaser6Month {
	Connection connection = null;
	int fileCounter=0;
	HashSet<String> holidays=new HashSet();
	private ArrayList<Ticker> buyList=null;
	
	private Date startDate;
	private Date endDate;
	private String monthToChase;
	private java.util.Date currentDay;
	
	private String startDateString=null;
	private String endDateString=null;
	private String performanceDateString=null;
	private String runDateString=null;
	private String restartDateString=null;
	private String reendDateString=null;
	private Date rebalstartDate;
	private Date rebalendDate;
	private Date rebRunDate;
	
	private String rebRunDateString=null;


	
	private boolean threeMonthRun=false;
	
	private void loadMomentumBuyList() throws SQLException{
		
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
			if(monthToChase.startsWith("FEB") || monthToChase.startsWith("MAR")){
				toTableName="bhav"+(startYear+1);
			}
		System.out.println("call fetchMomentumBuyList29('"+startDateString+"','"+endDateString+"','"+fromTableName+"','"+toTableName+"','"+monthToChase+"','"+runDateString+"',@curroc,@avgroc1)");
		String buylistquery="call fetchMomentumBuyList29(?,?,?,?,?,?,?,?)";
		stmt=connection.prepareCall(buylistquery);
		
		stmt.setString(1, startDateString);
		stmt.setString(2, endDateString);
		stmt.setString(3, fromTableName);
		stmt.setString(4, toTableName);
			stmt.setString(5, monthToChase);	
		stmt.setString(6, runDateString);
		stmt.registerOutParameter(7, java.sql.Types.DOUBLE);
		stmt.registerOutParameter(8, java.sql.Types.DOUBLE);
		stmt.execute();
		
		System.err.println("Average Roc is "+stmt.getDouble(7));
		
		System.err.println("Current Average Roc is "+stmt.getDouble(8));
		
		
		//update bhav2015 set closeindictor=case when close-PREVCLOSE >0 then 'P' else 'N' end where idBhav2015 > 0 and mktdate>='2015-01-01' and MKTDATE <='2015-12-31'
		
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
	    endDate=getEndDate(currentDay);
	    startDateString=formatter.format(startDate);
	    endDateString=formatter.format(endDate);
	    runDateString=formatter.format(currentDay);

	    Calendar cal1=Calendar.getInstance();
	    cal1.setTime(currentDay);
	    cal1.add(Calendar.DAY_OF_MONTH, 10);
	    rebalstartDate=getReStartDate(cal1.getTime());
	    rebalendDate=getEndDate(cal1.getTime());
	    restartDateString=formatter.format(rebalstartDate);
	    reendDateString=formatter.format(rebalendDate);
	  
	    rebRunDate=getRunDate(cal1.getTime());
	    
	    rebRunDateString=formatter.format(rebRunDate);

		//date for performance stats
	    
	    getFirstWorkingDayOfMonth(currentDay);
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
		                                   "user=root&password=admin");
		    
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
	    cal.add(Calendar.MONTH, -6);
	    cal.add(Calendar.DAY_OF_MONTH, -2);
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
	        cal.add(Calendar.DAY_OF_MONTH, 1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

	    }
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
	    cal.add(Calendar.MONTH, -1);
	    cal.add(Calendar.DAY_OF_MONTH, -2);
	    
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
	        cal.add(Calendar.DAY_OF_MONTH, -1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

	    }
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
	private void createPerformanceStats() throws SQLException{
		try{
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
		//System.err.println("Allocation for stocks -->"+stmt.getDouble(3)+"%");
		System.err.println("Completed");
		//connection.commit();
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		
		
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

	
	
	public static void main(String[] args)throws Exception{
		D30MomentumChaser6Month mom=new D30MomentumChaser6Month();
		mom.connect();
		mom.loadHolidays();
		String start="2007-01-01";
		
		boolean generatePerformanceStats=false;
		mom.threeMonthRun=false;
	  	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");

	  	  mom.currentDay=new java.util.Date(formatter.parse(start).getTime());  
Calendar cal=Calendar.getInstance();
cal.setTime(mom.currentDay);
java.util.Date monthoprocess;
		for (int i=0;i<124;i++){
			 monthoprocess=cal.getTime();
			 System.err.println("Processing "+monthoprocess);
			mom.setUpDates(monthoprocess);
			mom.loadMomentumBuyList();
			//mom.rebalancePortfolio();

			if(!generatePerformanceStats) {
				mom.createPerformanceStats();
			}
				
	cal.add(Calendar.MONTH, 1);
	}
		mom.closeconnection();
		
		
		
		
	}
}
