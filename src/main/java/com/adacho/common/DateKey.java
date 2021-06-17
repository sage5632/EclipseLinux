package com.adacho.common;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
public class DateKey implements WritableComparable<DateKey>{
	private String year;
	private Integer month;
	public DateKey() {
	}
	public DateKey(String year, Integer month) {
		this.year = year;
		this.month = month;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public Integer getMonth() {
		return month;
	}
	public void setMonth(Integer month) {
		this.month = month;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, year);
		out.writeInt(month);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		year = WritableUtils.readString(in);
		month = in.readInt();
	}
	@Override
	public int compareTo(DateKey key) {
		// TODO Auto-generated method stub
		int result = year.compareTo(key.year);
		if(result == 0) {
			result = month.compareTo(key.month);
		}
		return result;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return  new StringBuilder().append(year).append(",").append(month).toString();
	}
}