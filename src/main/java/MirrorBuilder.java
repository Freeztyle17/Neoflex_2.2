import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import java.util.Arrays;
import java.util.List;

public class MirrorBuilder {

    public static void main(String[] args) {
    
        SparkSession spark = SparkSession.builder()
                .appName("Mirror Builder")
                .master("local[*]")  
                .getOrCreate();

        String deltaPath = "data_deltas"; 
        String tableName = "md_account";
        String[] uniqueKeys = {"ACCOUNT_RK"};
        
        Dataset<Row> finalMirror = buildMirror(spark, deltaPath, tableName, uniqueKeys);
        
	String outputDir = "mirror_output_directory";
	saveMirror(finalMirror, outputDir);
	
	logProcess(spark, tableName, deltaPath);
	
       	spark.stop();
    }
    
    private static Dataset<Row> buildMirror(SparkSession spark, String deltaPath, String tableName, String[] uniqueKeys) {
    	Dataset<Row> deltas = spark.read()
    	.format("csv")
    	.option("header", true)
    	.option("delimiter", ";")
    	.option("inferSchema", true)
    	.load(deltaPath + "/*");

    
	Dataset<Row> finalMirror = deltas.dropDuplicates(uniqueKeys);
    		
    		
    	return finalMirror;
    }
    
    private static void saveMirror(Dataset<Row> mirrorData, String outputDir) {
    	mirrorData.write().mode("overwrite").option("header", true).csv(outputDir);
    }
    
    private static void logProcess(SparkSession spark, String tableName, String deltaPath) {
    	Dataset<Row> logData = spark.createDataFrame(
    		java.util.Arrays.asList(new LogEntry(tableName, deltaPath, java.time.LocalDateTime.now())), LogEntry.class);
    		
    	logData.write().mode("append").option("header", true).csv("logs/log_directory");
    }
}	
