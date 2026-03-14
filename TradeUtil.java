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

public class TradeUtil {
	static Connection connection = null;
	static ArrayList<String> mktdates = null;
	static HashSet<String> holidays=new HashSet();

	public static Connection connect() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection("jdbc:mysql://localhost/bhav?" + "user=root&password=root&allowMultiQueries=true");

			System.out.println("Connected !");
		} catch (Exception ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			ex.printStackTrace();
			// System.out.println("SQLState: " + ex.getSQLState());
			// System.out.println("VendorError: " + ex.getErrorCode());
		}
		return connection;

	}
	
public static void loadHolidays() throws SQLException{
		
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

	private boolean isPrevDayHoliday(java.util.Date date){
		boolean isHoliday=false;
		
		DateFormat destDf = new SimpleDateFormat("dd-MMM-yy");
		            // format the date into another format

		           String dateStr = destDf.format(date);

		             

		
		isHoliday=holidays.contains(dateStr);
		return isHoliday;
		
	}
	public java.util.Date getStartDate(java.util.Date date) {
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
	
	public static ArrayList<String> loadMarketDates(String startdate, String enddate) throws SQLException {
		String sql = "select distinct mktdate from mktdatecalendar where mktdate>=? and mktdate<=? order by mktdate asc";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, startdate);
		prep.setString(2, enddate);
		ResultSet rs = prep.executeQuery();
		mktdates = new ArrayList<String>();
		while (rs.next()) {
			mktdates.add(rs.getString(1));
		}
		return mktdates;
	}
	public static String getPreviousDay(String today,Connection connection) throws SQLException {
		String sql = "select distinct mktdate from mktdatecalendar where mktdate<? order by mktdate desc limit 1";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, today);
		String prevday = null;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			prevday = rs.getString(1);
			;
		}
		return prevday;
	}

	
	
	public static double[] getTodayDeliveryVolume(String today,String symbol,Connection connection,String bhavyear) throws SQLException {
		String sql = "select deliveryvolume,round(( deliveryvolume/volume * 100 ),2) as deliverypercent from "+bhavyear+" where symbol=? and mktdate=? ;";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, symbol);
		
		prep.setString(2, today);

		double maxDelivery[] = new double[2];
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			maxDelivery[0] = rs.getDouble(1);
			maxDelivery[1] = rs.getDouble(2);

			;
		}
		return maxDelivery;
	}

	public static double getMaxDeliveryVolume(String lookback,String today,String symbol,Connection connection,String bhavyear) throws SQLException {
		String sql = "select max(deliveryvolume) as maxdelivery from "+bhavyear+" where symbol=? and mktdate>=? and mktdate<=?";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, symbol);
		prep.setString(2, lookback);
		prep.setString(3, today);

		double maxDelivery = 0;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			maxDelivery = rs.getDouble(1);
			;
		}
		return maxDelivery;
	}
	
	public static String getTenDayBack(String today,Connection connection) throws SQLException {
		String sql = "select mktdate from (select distinct mktdate from mktdatecalendar where mktdate<=? order by mktdate desc limit 10)m order by mktdate asc limit 1;";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, today);
		String prevday = null;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			prevday = rs.getString(1);
			;
		}
		return prevday;
	}
	
	public static String getFiveDayBack(String today,Connection connection) throws SQLException {
		String sql = "select mktdate from (select distinct mktdate from mktdatecalendar where mktdate<? order by mktdate desc limit 5)m order by mktdate asc limit 1;";
		PreparedStatement prep = connection.prepareStatement(sql);
		prep.setString(1, today);
		String prevday = null;
		ResultSet rs = prep.executeQuery();
		while (rs.next()) {
			prevday = rs.getString(1);
			;
		}
		return prevday;
	}
			

	public static double getTightClosesCount(String symbol,String seqstart,String seqend,Connection connection) throws SQLException,Exception {
		
		String fromTableName=null;
		String toTableName=null;
		 SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");

	  	  Date startdate=new java.util.Date(formatter.parse(seqstart).getTime());  
	  	Date enddate=new java.util.Date(formatter.parse(seqend).getTime());  
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(startdate);

		int startYear=cal.get(Calendar.YEAR);
		cal.setTime(enddate);
		int endYear=cal.get(Calendar.YEAR);
		fromTableName="bhav"+startYear;
		toTableName="bhav"+endYear;
		double threshold=8;
		CallableStatement stmt=null;
		double tightcloses=0;
		try{
			//call fetchTightClosesForthePeriod('ESCORTS','2020-02-05',@yesterday,'bhav2019','bhav2020',8,@curroc)
		System.out.println("call fetchTightClosesForthePeriod('"+symbol+"','"+seqstart+"','"+seqend+"','"+fromTableName+"','"+toTableName+"','"+threshold+"',@curroc)");
		String buylistquery="call fetchTightClosesForthePeriod(?,?,?,?,?,?,?)";
		stmt=connection.prepareCall(buylistquery);
		stmt.setString(1, symbol);

		stmt.setString(2, seqstart);
		stmt.setString(3, seqend);
		stmt.setString(4, fromTableName);
		stmt.setString(5, toTableName);
			stmt.setDouble(6, threshold);	
		stmt.registerOutParameter(7, java.sql.Types.DOUBLE);
		stmt.execute();
		 
		System.err.println(symbol+" Threshold is "+stmt.getDouble(7));
		
		
		
		//update bhav2015 set closeindictor=case when close-PREVCLOSE >0 then 'P' else 'N' end where idBhav2015 > 0 and mktdate>='2015-01-01' and MKTDATE <='2015-12-31'
		
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}finally {
			stmt.close();
			

	}
		return tightcloses;
}
}
