public class LogEntry implements java.io.Serializable {
	private String tableName;
	private String deltaPath;
	private java.time.LocalDateTime timestamp;
	
	public LogEntry(String tableName, String deltaPath, java.time.LocalDateTime timestamp) {
		this.tableName = tableName;
		this.deltaPath = deltaPath;
		this.timestamp = timestamp;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public String getDeltaPath(){
		return deltaPath;
	}
	
	public java.time.LocalDateTime getTimestamp(){
		return timestamp;
	}
}
