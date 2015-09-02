package Vol;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class COMPUTE extends EvalFunc<String> {
	String FileName;
	@Override
	public String exec(Tuple tuple) throws IOException {
		// {(AAPL,2014,12,-0.04074),(AAPL,2014,11,0.0918),...}
		int months = 0;
		double sum = 0.0, sumsq = 0.0;
		ArrayList<Double> x_i = new ArrayList<Double>();

		if (tuple == null || tuple.size() == 0) {
			return null;
		}

		try {
			DataBag bag = (DataBag) tuple.get(0);
			for (Tuple t : bag) {
				x_i.add((Double) t.get(3));
				sum = sum + (Double) t.get(3);
				months++;
			}

			double x_bar = sum / months;

			for (int i = 0; i < x_i.size(); i++) {
				sumsq = sumsq + (x_i.get(i) - x_bar) * (x_i.get(i) - x_bar);
			}
			double val1 = sumsq / (months - 1);

			double vol = Math.sqrt(val1);

			if(vol>0.0){
    	 	 		return FileName+"\t"+ String.valueOf(vol);
    	 	 	}
    	 	 	else{
    	 	 		return null;
    	 	 	}

		} catch (Exception e) {
			throw new IOException("Something bad happened here", e);
		}

	}

}
