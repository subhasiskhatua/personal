package com.subhasis.personal.custom.mapper;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import com.subhasis.personal.model.Book;
import com.subhasis.personal.model.BookJson;

public class CustomMapperEncoder {
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
		CustomMapperEncoder app = new CustomMapperEncoder();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("CSV to Dataset<Book> as JSON").master("local")
				.getOrCreate();

		String filename = "resources/book/books.csv";
		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true").option("header", "true")
				.load(filename);
		df.show();

		Dataset<String> bookDf = df.map(new BookMapper(), Encoders.STRING());
		bookDf.show(2,10);

		Dataset<Row> bookAsJsonDf = spark.read().json(bookDf);
		bookAsJsonDf.show();
	}
}