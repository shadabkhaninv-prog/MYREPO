package bhav;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
public class MomentumChaser9Month {
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
			if(threeMonthRun){
				
				if(monthToChase.startsWith("FEB") || monthToChase.startsWith("MAR")){
					toTableName="bhav"+(startYear+1);
					
				}
				System.out.println("call fetchBuyListthreemonthfix('"+startDateString+"','"+endDateString+"','"+fromTableName+"','"+toTableName+"','"+monthToChase+"','"+runDateString+"')");
			}else{
				System.out.println("call fetchBuyListthreemonthfix('"+startDateString+"','"+endDateString+"','"+fromTableName+"','"+toTableName+"','"+monthToChase+"','"+runDateString+"')");
			}
			String buylistquery="call fetchBuyListthreemonthfix(?,?,?,?,?,?)";
			stmt=connection.prepareCall(buylistquery);
			
			stmt.setString(1, startDateString);
			stmt.setString(2, endDateString);
			stmt.setString(3, fromTableName);
			stmt.setString(4, toTableName);
			stmt.setString(5, monthToChase);	
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
	    cal.add(Calendar.MONTH, -9);
	    
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
		stmt.execute();
		//connection.commit();
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		
		
	}
	
	
	public static void main(String[] args)throws Exception{
		MomentumChaser9Month mom=new MomentumChaser9Month();
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
		for (int i=0;i<125;i++){
			 monthoprocess=cal.getTime();
			 System.err.println("Processing "+monthoprocess);
			mom.setUpDates(monthoprocess);
			mom.loadMomentumBuyList();
			if(!generatePerformanceStats) {
				mom.createPerformanceStats();
			}
				
	cal.add(Calendar.MONTH, 1);
	}
		mom.closeconnection();
		
		
		
		
	}
}
