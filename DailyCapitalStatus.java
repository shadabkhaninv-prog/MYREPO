package bhav;

public class DailyCapitalStatus {
	private double investedCapital;
	private double availableCapital;
	public double getInvestedCapital() {
		return investedCapital;
	}
	public void setInvestedCapital(double investedCapital) {
		this.investedCapital = investedCapital;
	}
	public double getAvailableCapital() {
		return availableCapital;
	}
	public void setAvailableCapital(double availableCapital) {
		this.availableCapital = availableCapital;
	}
	public String getMktdate() {
		return mktdate;
	}
	public void setMktdate(String mktdate) {
		this.mktdate = mktdate;
	}
	private String mktdate;
	
}
