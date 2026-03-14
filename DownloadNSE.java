package work;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.*;
import java.nio.channels.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.io.FileUtils;;

public class DownloadNSE {
	private java.util.Date currentDay;
	Connection connection = null;
	static String startdate = null;

	DownloadNSE(String startdate) {
		this.startdate = startdate;
	}

	DownloadNSE() {

	}

	private void connect() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection =
				       DriverManager.getConnection("jdbc:mysql://localhost/bhav?" +
				                                   "enabledTLSProtocols=TLSv1.2&useSSL=false&allowPublicKeyRetrieval=true&user=root&password=root");
				 
			System.out.println("Connected !");
		} catch (Exception ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			ex.printStackTrace();
			// System.out.println("SQLState: " + ex.getSQLState());
			// System.out.println("VendorError: " + ex.getErrorCode());
		}

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

	void downloadDeliveryFile() throws SQLException,Exception {
		connect();
		// startdate="2020-05-15";

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatter2 = new SimpleDateFormat("ddMMyyyy");
		
		SimpleDateFormat formatter3 = new SimpleDateFormat("yyyyMMdd");


		// startdate=formatter.format(mom.currentDay);

		currentDay = new java.util.Date(formatter.parse(startdate).getTime());
		System.err.println(currentDay);

		Calendar cal = Calendar.getInstance();
		cal.setTime(currentDay);
		String bhavdate=null;
		for (int i = 0; i < 1; i++) {

			String endDateString = formatter2.format(cal.getTime());
			startdate = formatter.format(cal.getTime());
			bhavdate = formatter3.format(cal.getTime());

			
			System.err.println("Downloading bhavcopy and Delivery data for " + endDateString);
			String fileName = "MTO_" + endDateString + ".DAT";
			String bhavcopy = startdate + "-NSE-EQ.txt";

			String bhavfilename = "BhavCopy_NSE_CM_0_0_0_" + bhavdate + "_F_0000.csv.zip";
// https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20240828_F_0000.csv.zip
			formatter.format(currentDay);
			//URL website = new URL("https://www1.nseindia.com/archives/equities/mto/" + fileName);
			// 2024-08-28-NSE-EQ
			//javascript:;
			
			URL website = new URL("https://archives.nseindia.com/archives/equities/mto/" + fileName);
			URL bhavwebsite = new URL("https://nsearchives.nseindia.com/content/cm/" + bhavfilename);
			
		System.err.println("bhavcopy URL " + bhavwebsite);
		
		System.err.println("delivery URL " + website);

//			try {
//			FileUtils.copyURLToFile(bhavwebsite, new File("E:\\marketdata\\test\\"),3000,3000);
//			}catch(Exception e) {
//				e.printStackTrace();
//			}
			FileUtils.copyURLToFile(website, new File("E:\\deliverydata\\test\\" + fileName));

			
			DeliveryDataProcessor obj=new DeliveryDataProcessor();
			obj.connect();
			obj.loadHolidays();	
			obj.processData();

//			currentDay = new java.util.Date(formatter.parse(getNextDay(startdate)).getTime());
//			cal.setTime(currentDay);

			// cal.add(Calendar.DATE, 1);

		}

	}

	public static void main(String[] args) throws Exception {
		DownloadNSE mom = new DownloadNSE();
		mom.connect();
	//	 startdate="2020-09-28";

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatter2 = new SimpleDateFormat("ddMMyyyy");

		// startdate=formatter.format(mom.currentDay);
		System.err.println(mom.currentDay);
		System.err.println("Start date is "+startdate);

		
//		mom.currentDay = new java.util.Date(formatter.parse(startdate).getTime());
//		System.err.println(mom.currentDay);

		Calendar cal = Calendar.getInstance();
		cal.setTime(mom.currentDay);

		for (int i = 0; i < 1; i++) {

			String endDateString = formatter2.format(cal.getTime());
		//	startdate = formatter.format(cal.getTime());
			System.err.println("Processing Delivery data for " + endDateString);
			String fileName = "MTO_" + endDateString + ".DAT";
			String bhavfileName = "BhavCopy_NSE_CM_0_0_0_" + endDateString + ".csv.zip";

			//bhav copy download https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20240827_F_0000.csv.zip
			
			formatter.format(mom.currentDay);
			URL bhavwebsite = new URL("https://archives.nseindia.com/content/historical/EQUITIES/2024/APR/cm24APR2024bhav.csv.zip/" + bhavfileName);
			URL website = new URL("https://archives.nseindia.com/archives/equities/mto/" + fileName);

			FileUtils.copyURLToFile(website, new File("H:\\deliverydata\\test\\" + fileName));

//			mom.currentDay = new java.util.Date(formatter.parse(mom.getNextDay(startdate)).getTime());
//			cal.setTime(mom.currentDay);

			// cal.add(Calendar.DATE, 1);

		}

		//
		// ReadableByteChannel rbc = Channels.newChannel(website.openStream());
		// FileOutputStream fos = new FileOutputStream("information.html");
		// fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
	}
}
