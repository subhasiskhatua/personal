package com.subhasis.personal.custom.mapper;

import java.util.ArrayList;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import com.subhasis.personal.model.CustAttBean;
import com.subhasis.personal.model.CustattDouble;
import com.subhasis.personal.model.CustattString;

import scala.collection.mutable.WrappedArray;

public class CustomMapperEncoder2 {

	static class CustAttMapper implements MapFunction<Row, CustAttBean> {

		private static final long serialVersionUID = 3825337397393395833L;

		@Override
		public CustAttBean call(Row value) throws Exception {
			CustAttBean bean = new CustAttBean();
			bean.setId((String) value.getAs("id"));
			WrappedArray<CustattString> wr = value.getAs("pipeline_custatt_string");

			if (wr.length() > 0) {
				ArrayList<CustattString> cust_str_list = new ArrayList<CustattString>();
				scala.collection.immutable.List<CustattString> list = wr.toList();
				for(int i=0; i<list.size(); i++) {
					Object obj = list.apply(i);
					System.out.println(obj.getClass().getName());
					if(obj instanceof GenericRowWithSchema) {
						GenericRowWithSchema row = (GenericRowWithSchema) obj;
						String key = row.getAs("key");
						String val = row.getAs("value");
						System.out.println(key + " : " + val);
						CustattString custStr = new CustattString();
						custStr.setKey(key);
						custStr.setValue(val);
						cust_str_list.add(custStr);
					}
					
				}

				bean.setPipeline_custatt_string(cust_str_list);
			}
			
			WrappedArray<CustattDouble> wrDbl = value.getAs("pipeline_custatt_double");

			if (wrDbl.length() > 0) {
				ArrayList<CustattDouble> cust_dbl_list = new ArrayList<CustattDouble>();
				scala.collection.immutable.List<CustattDouble> list = wrDbl.toList();
				for(int i=0; i<list.size(); i++) {
					Object obj = list.apply(i);
					System.out.println(obj.getClass().getName());
					if(obj instanceof GenericRowWithSchema) {
						GenericRowWithSchema row = (GenericRowWithSchema) obj;
						String key = row.getAs("key");
						ArrayList<Long> dblValueList = new ArrayList<Long>();
						WrappedArray<Double> wrDblVal = row.getAs("value");
						if (wrDblVal.length() > 0) {
							scala.collection.immutable.List<Double> listDbl = wrDblVal.toList();
							for(int j=0; j<listDbl.size(); j++) {
								Object obj2 = listDbl.apply(j);
								System.out.println(obj2.getClass().getName());
								if(obj2 instanceof Long) {
									Long dblVal = (Long)obj2;
									dblValueList.add(dblVal);
									System.out.println(key + " : " + dblVal);
								}
								
							}
						}
						CustattDouble custDbl = new CustattDouble();
						custDbl.setKey(key);
						custDbl.setValue(dblValueList);
						cust_dbl_list.add(custDbl);
					}
					
				}

				bean.setPipeline_custatt_double(cust_dbl_list);
			}
			
			return bean;
		}

	}

	public static void main(String[] args) {
		CustomMapperEncoder2 app = new CustomMapperEncoder2();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("JSON Custom Mapper").master("local").getOrCreate();

		String filename = "resources/custom-mapper-encoder-2.json";
		Dataset<Row> df = spark.read().format("json").option("multiLine", true).option("inferSchema", "true")
				.option("header", "true").load(filename);
		df.show();

		df.printSchema();

		Dataset<CustAttBean> bookDf = df.map(new CustAttMapper(), Encoders.bean(CustAttBean.class));
		//bookDf.show(2, 10);
		bookDf.show();
	}
}