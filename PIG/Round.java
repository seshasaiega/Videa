package roundEval;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

 
public class Round extends EvalFunc<String> {
		
	 public String exec(Tuple input) throws IOException {
	        if (input.size() != 3 || input == null || input.size() == 0)
	               return null;
	        try {
	        		// Read value to be rounded
	        		BigDecimal value1 = new BigDecimal((String) input.get(0) );
	        		BigDecimal value2 = new BigDecimal((String) input.get(1) );
	        		
	        		int places = (int) input.get(2);
	        		
	        		BigDecimal product = value1.multiply(value2);
	        		
	        		
	        		return ""+product.setScale(places, BigDecimal.ROUND_HALF_UP);
	        		
	        		

	        } catch (Exception e) {
	            // TODO: handle exception
	            throw WrappedIOException.wrap(
	                    "Caught exception processing input row ", e);
	        }
	       
	    }
}
