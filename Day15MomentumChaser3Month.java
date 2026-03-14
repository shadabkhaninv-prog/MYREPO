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
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import javax.swing.text.DateFormatter;
public class Day15MomentumChaser3Month {
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
	private String runDateString=null;
	private java.util.Date endRunDate=null;
	private String performanceDateString=null;
	private String endRunDateString=null;
	private int weeknumber;
	
	
	private boolean threeMonthRun=false;
	private java.util.Date monthoprocess;
	
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
		    Calendar rundate = Calendar.getInstance();
		    rundate.setTime(monthoprocess);
		    weeknumber=rundate.get(Calendar.WEEK_OF_MONTH);
		    System.err.println(monthToChase);
		    //System.err.println(weeknumber);
		    

			if(monthToChase.startsWith("FEB") || monthToChase.startsWith("MAR") || (monthToChase.startsWith("JAN") && weeknumber >2)){
				toTableName="bhav"+(startYear+1);
				//System.err.println("Jan Week number is " +rundate.get(Calendar.WEEK_OF_MONTH));

			}
			if(weeknumber>2){
				
				System.out.println("Calling fetchFortnightly3MList("+startDateString+"',"+endDateString+","+fromTableName+","+toTableName+",Mid"+monthToChase+",'"+runDateString+"')");
			}else{
		System.out.println("Calling fetchFortnightly3MList("+startDateString+","+endDateString+","+fromTableName+","+toTableName+","+monthToChase+",'"+runDateString+"')");
			}
		String buylistquery="call fetchFortnightly3MList(?,?,?,?,?,?)";
		stmt=connection.prepareCall(buylistquery);
		
		stmt.setString(1, startDateString);
		stmt.setString(2, endDateString);
		stmt.setString(3, fromTableName);
		stmt.setString(4, toTableName);
		if(weeknumber>2){
		stmt.setString(5, "Mid"+monthToChase);
		}else{
			stmt.setString(5, monthToChase);	
		}
		stmt.setString(6, runDateString);
		stmt.execute();
		
		//update bhav2015 set closeindictor=case when close-PREVCLOSE >0 then 'P' else 'N' end where idBhav2015 > 0 and mktdate>='2015-01-01' and MKTDATE <='2015-12-31'
		System.err.println("Completed");
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
	    
	    Calendar calrun=Calendar.getInstance();
	    calrun.setTime(currentDay);

	    if(calrun.get(Calendar.DAY_OF_MONTH)>14){
	    	calrun.set(Calendar.DAY_OF_MONTH,calrun.getActualMaximum(Calendar.DAY_OF_MONTH));	
	    }else{
	    	calrun.add(Calendar.DAY_OF_MONTH, 14);
	    }
		endRunDate=calrun.getTime();
		endRunDateString=formatter.format(endRunDate);
		

	    
		//date for performance stats
	    
	    getFirstWorkingDayOfMonth(currentDay);
	    performanceDateString=formatter.format(getFirstWorkingDayOfMonth(currentDay));
	    
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
	    cal.add(Calendar.MONTH, -2);
	    
	    
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
	        cal.add(Calendar.DAY_OF_MONTH, 1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

	    }
	    System.err.println(cal.getTime());
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
	    System.err.println(cal.getTime());
	    return cal.getTime();
	}

	
	public java.util.Date getEndDate(java.util.Date date) {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);
	    cal.add(Calendar.MONTH, -1);
	    cal.add(Calendar.DAY_OF_MONTH, -1);
	    
	    int dayOfWeek;
	    dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime())){
	        cal.add(Calendar.DAY_OF_MONTH, -1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

	    }
	    System.err.println(cal.getTime());
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
		buylistquery="call calculatePerformanceFortnightly(?,?,?)";
		stmt=connection.prepareCall(buylistquery);
			
			
			if(weeknumber>2){
				stmt.setString(1, "Mid"+monthToChase);
				System.out.println("call calculatePerformanceFortnightly('Mid"+monthToChase+"','"+runDateString+"','"+endRunDateString+"')");
			}else{
				stmt.setString(1, monthToChase);
				System.out.println("call calculatePerformanceFortnightly('"+monthToChase+"','"+runDateString+"','"+endRunDateString+"')");
			}
			stmt.setString(2, runDateString);
			stmt.setString(3, endRunDateString);
		
		
		stmt.execute();
		//connection.commit();
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		
		
	}

	
	
	public static void main(String[] args)throws Exception{
		Day15MomentumChaser3Month mom=new Day15MomentumChaser3Month();
		mom.connect();
		mom.loadHolidays();
		String start="2007-01-01";
		
		boolean generatePerformanceStats=true;
		mom.threeMonthRun=false;
	  	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");

	  	  mom.currentDay=new java.util.Date(formatter.parse(start).getTime());  
Calendar cal=Calendar.getInstance();
cal.setTime(mom.currentDay);

		for (int i=0;i<48;i++){
			 mom.monthoprocess=cal.getTime();
			 System.err.println("Processing "+mom.monthoprocess);
			mom.setUpDates(mom.monthoprocess);
			mom.loadMomentumBuyList();
			if(generatePerformanceStats) {
				mom.createPerformanceStats();
			}
				
			if(mom.weeknumber>2){
				cal.add(Calendar.MONTH, 1);
				cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
			}else{
	cal.add(Calendar.DAY_OF_MONTH, 14);
			}
	
	}
		mom.closeconnection();
		
		
		
		
	}
}
