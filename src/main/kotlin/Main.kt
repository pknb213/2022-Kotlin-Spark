import org.jetbrains.kotlinx.spark.api.*

fun main(args: Array<String>) {
    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
    val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("Simple Application").orCreate
//    println(MapAndListOperation().app(spark))
    spark.toDS("a" to 1, "b" to 2)
}