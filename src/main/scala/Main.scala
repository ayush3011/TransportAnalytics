import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, broadcast, col, count, hour, lag, max, row_number, sum}

case class Trip(
                 trip_id: Int,
                 vehicle_id: String,
                 driver_id: String,
                 route_id: String,
                 city: String,
                 distance_km: Double,
                 fare_amount: Int,
                 trip_date: java.sql.Date,
                 start_time: java.sql.Timestamp,
                 end_time: java.sql.Timestamp
               )

case class Vehicle(
                    capacity: Long,
                    fuel_type: String,
                    vehicle_id: String,
                    vehicle_type: String
                  )

object Main {
  def main(args: Array[String]): Unit = {
    /*
    Section 1 : Scala Basics

      --> val vs var
      val is immutable (cannot be reassigned after creation),
      whereas var is mutable and can be updated.

      --> Case Class
      A case class is a special Scala class used to store immutable data.
      It automatically provides constructors, getters, toString, equals,
      and pattern matching support.

      --> Higher-Order Functions
      Higher-order functions are functions that take other functions
      as parameters or return functions as results.
  */
    val fares = List(150, 100, 180, 250, 80, 300, 130, 220, 90, 350)
    val filteredTrips = fares.filter(fare => fare > 150)
    val totalFare = fares.sum
    val avgFare = totalFare / fares.size
    val maxFare = fares.max

    //    println(">150 fare : " + filteredTrips)
    //    println("Total Fare : " + totalFare)
    //    println("Average Fare : " + avgFare)
    //    println("Max Fare : " + maxFare)


    /*
      Section 2: Spark Basics
        - Create SparkSession.
        - Read CSV and JSON datasets.
        - Perform select, filter, and create derived columns.
     */
    val spark = SparkSession.builder()
      .appName("TransportAnalytics")
      .master("local[*]")
      .getOrCreate()

    val tripDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/trip_data.csv")

    val vehicleDF = spark.read
      .option("multiline", "true")
      .json("data/vehicle_data.json")

    import spark.implicits._
    //    tripDF.select($"trip_id", $"fare_amount").show()
    //    vehicleDF.select("vehicle_id", "vehicle_type").show()
    //    tripDF.select(col("trip_id"), col("fare_amount")).show()
    //    tripDF.filter($"fare_amount">200).show();
    //    tripDF.withColumn("fare_per_km", $"fare_amount"/$"distance_km").show()


    /*
    Section 3: Transformations & Joins
        - Join trip and vehicle datasets.
        - Create fare_per_km column.
    */
    //    val joinedDF = tripDF.join(vehicleDF, Seq("vehicle_id"), "inner")
    //    joinedDF.withColumn("fare_per_km", $"fare_amount"/$"distance_km").show()


    /*
     Section 4: Aggregations
      - Total trips per city.
      - Average fare per city.
      - Maximum distance per driver.
      - Aggregations per vehicle type.
    */

    //    tripDF.groupBy($"city")
    //      .agg(count($"*").as("trips_per_city"))
    //      .show()
    //
    //    tripDF.groupBy($"city")
    //      .agg(avg($"fare_amount").as("avg_fare_per_city"))
    //      .show()
    //
    //    tripDF.groupBy($"driver_id")
    //      .agg(max($"distance_km").as("max_dist_per_driver"))
    //      .show()
    //
    //    vehicleDF.groupBy($"vehicle_type")
    //      .agg(count($"*").as("vehicle_type_count"))
    //      .show()


    /*
    Section 5: Window Functions
      - Top 2 highest fare trips per city.
      - Running total of fare per driver.
      - Previous trip fare using lag.
    */

    //    val window1 = Window.partitionBy($"city").orderBy($"fare_amount".desc)
    //    tripDF.withColumn("rank", row_number().over(window1))
    //      .filter($"rank" <= 2)
    //      .show()
    //
    //    val window2 = Window.partitionBy($"driver_id").orderBy($"trip_date")
    //    tripDF.withColumn("running_total_fare", sum($"fare_amount").over(window2))
    //      .show()
    //
    //    val window3 = Window.partitionBy($"driver_id").orderBy($"trip_date")
    //    tripDF.withColumn("previous_trip_fare", lag($"fare_amount", 1).over(window3))
    //      .show()

    /*
    Section 6: Business Scenarios
      - Find most profitable route.
      - Identify drivers with frequent short trips.
      - Find peak hour for trips.
     */

    //    tripDF.groupBy($"route_id")
    //      .agg(sum($"fare_amount").as("fare_per_route"))
    //      .orderBy($"fare_per_route".desc)
    //      .show();
    //
    //    val shortTripSize = 10;
    //    tripDF.filter($"distance_km" < shortTripSize)
    //      .groupBy($"driver_id")
    //      .agg(count($"*").as("count_of_frequent_short_trips"))
    //      .orderBy($"count_of_frequent_short_trips".desc)
    //      .show()
    //
    //        tripDF
    //          .groupBy(hour($"start_time").as("hour"))
    //          .agg(count($"*").as("trip_count"))
    //          .orderBy($"trip_count".desc)
    //          .show()

    /*
    Section 7: Bonus
      - Write data partitioned by city in parquet format.
      - Explain caching and broadcast joins.
      Parquet:
        columnar storage file format optimized for fast data retrieval
         and high-performance analytics
      Caching:
          store in RAM to avoid recomputing the join over df

      Broadcast Join:
          Copies a small DataFrame to all executors to avoid
          expensive shuffling during joins.

          Without cache:
            Join -> Show
            Join -> Count
            Join -> GroupBy

          With cache:
            Join once -> store in RAM
            Show
            Count
            GroupBy
     */

    // Parquet
    //    tripDF.write
    //      .mode("overwrite")
    //      .partitionBy("city")
    //      .parquet("output/trips_by_city")
    //
    //    println("Partitioned parquet write successful")
    //
    //    val partitionedDF = spark.read
    //      .parquet("output/trips_by_city")
    //
    //    partitionedDF.show()
    //
    //
    //    // Caching
    //    val joinedDF = tripDF.join(
    //      vehicleDF,
    //      Seq("vehicle_id"),
    //      "inner"
    //    )
    //
    //    joinedDF.cache()
    //
    //
    //    // Broadcast
    //    val broadcastJoinedDF = tripDF.join(
    //      broadcast(vehicleDF),
    //      Seq("vehicle_id"),
    //      "inner"
    //    )
    //
    //    broadcastJoinedDF.show()
  }
}

