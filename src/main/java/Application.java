import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {

        StructType schema = new StructType()
                .add("recordId", DataTypes.IntegerType)
                .add("callDateTime", DataTypes.StringType)
                .add("priority", DataTypes.StringType)
                .add("district", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("callNumber", DataTypes.StringType)
                .add("incidentLocation", DataTypes.StringType)
                .add("location", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("PoliceCalls").getOrCreate();

        Dataset<Row> rawData = sparkSession.read().option("header",true).schema(schema).csv("C:\\Users\\Ferhat\\Desktop\\Data Engineering\\police911.csv");

        Dataset<Row> filteredData = rawData.filter(rawData.col("recordId").isNotNull());
        filteredData.show();
        //System.out.println(filteredData.count());

        filteredData.groupBy("district").count().sort(functions.desc("count")).show(50 );
    }
}
