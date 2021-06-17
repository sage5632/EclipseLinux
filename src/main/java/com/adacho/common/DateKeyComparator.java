package com.adacho.common;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {
	protected DateKeyComparator() {
		super(DateKey.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;
		int cmp = k1.getYear().compareTo(k2.getYear());
		if (cmp != 0) {
			return cmp;
		}
		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
	}
}