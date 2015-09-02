package hw3;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class XI extends EvalFunc<String> {
	@Override
	String FileName;
	public String exec(Tuple tuple) throws IOException {
		// {(AAPL,2014,12,31,109.95)),(AAPL,2014,12,30,108.25),...}
		int min = 32, max = 0;
		double x_i = 0.0;
		double month_end = 0.0;
		double month_begin = 0.0;

		if (tuple == null || tuple.size() == 0) {
			return null;
		}

		try {
			// TODO Auto-generated method stub
			DataBag bag = (DataBag) tuple.get(0);

			for (Tuple t : bag) {
				FileName= String.valueOf(t.get(0));
				int day = Integer.parseInt(String.valueOf(t.get(3)));
				double val = Double.parseDouble(String.valueOf(t.get(4)));

				if (day < min) {
					month_begin = val;
					min = day;
				}
				if (day > max) {
					month_end = val;
					max = day;
				}
			}

			double x_i = (month_end - month_begin) / month_begin;
			
			return FileName.split("\\.")[0]+"\t"+String.valueOf(x_i); 

		} catch (Exception e) {
			throw new IOException("Something bad happened here", e);
		}

	}
}
