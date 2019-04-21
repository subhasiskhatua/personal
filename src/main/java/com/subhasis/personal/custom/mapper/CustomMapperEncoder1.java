package com.subhasis.personal.custom.mapper;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import com.subhasis.personal.model.Book;
import com.subhasis.personal.model.BookJson;

public class CustomMapperEncoder1 {
	static class BookMapper implements MapFunction<Row, String> {
		private static final long serialVersionUID = -8940709795225426457L;

		@Override
		public String call(Row value) throws Exception {
			Book b = new Book();
			b.setId((Integer)value.getAs("id"));
			b.setAuthorId((Integer)value.getAs("authorId"));
			b.setLink((String)value.getAs("link"));
			SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
			String stringAsDate = value.getAs("releaseDate");
			if (stringAsDate == null) {
				b.setReleaseDate(null);
			} else {
				b.setReleaseDate(parser.parse(stringAsDate));
			}
			b.setTitle((String)value.getAs("title"));

			BookJson bj = new BookJson();
			bj.setBook(b);
			JSONObject jo = bj.getBook();

			return jo.toString();
		}
	}

	public static void main(String[] args) {
		CustomMapperEncoder1 app = new CustomMapperEncoder1();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("JSON Custom Mapper").master("local")
				.getOrCreate();

		String filename = "resources/custom-mapper-encoder-1.json";
		Dataset<Row> df = spark.read().format("json").option("multiLine", true).option("inferSchema", "true").option("header", "true")
				.load(filename);
		df.show();
		
		df.printSchema();

		// Creates a temporary view using the DataFrame
		df.createOrReplaceTempView("menu");
		
		Dataset<Row> personPositions = df.select(df.col("name").as("menuName"),
				org.apache.spark.sql.functions.explode(df.col("menu")).as("menu"));

		Dataset<Row> test = personPositions.select(personPositions.col("menuName"),
				personPositions.col("menu").getField("length").as("menuLength"),
				personPositions.col("menu").getField("value").as("menuValue"));	
		
		test.show();
		test.createOrReplaceTempView("menu");
		
		Dataset<Row> res = spark.sql("SELECT menuValue from menu where menuLength > 99 AND menuLength < 299");
		res.show();
		
		Dataset<Row> arr1 = df.select(df.col("menu").getItem(1));
		arr1.show();
		
		//df.select(df.col("menu")).
		
		//Go for custom mapper for "menu", like BookMapper above
		
		
		// SQL statements can be run by using the sql methods provided by spark
		//Dataset<Row> namesDF = spark.sql("SELECT menu.length from menu EXTERNAL VIEW explode(menu) menutable as menu");
		//namesDF.show();
		//SELECT id, part.lock, part.key FROM mytable EXTERNAL VIEW explode(parts) parttable AS part;
	}
}