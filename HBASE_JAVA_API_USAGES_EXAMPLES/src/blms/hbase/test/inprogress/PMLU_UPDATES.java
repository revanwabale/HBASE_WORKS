package blms.hbase.test.inprogress;
/*
 *compile with
 * javac -classpath pig-0.16.0.2.6.0.3-8-core-h2.jar:`hadoop classpath` pmlu_update.java
 */

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;  
import org.apache.pig.data.TupleFactory;
 
public class PMLU_UPDATES extends EvalFunc<DataBag>{
	
	public DataBag exec(Tuple input) throws ExecException {
     	//get input bag 0: pmlu {(),(),..}
		//get input bag 1: packinfo {()}

		// expect one string
	    if (input == null || input.size() != 2) {
	        throw new IllegalArgumentException("pmlu_update udf: requires 2 input parameters.");
	    }
		
		DataBag bagFromPigPmlu = (DataBag) input.get(0);
		DataBag bagFromPigPackinfo = (DataBag) input.get(1);
				
        //ArrayList of Strings to hold the tuples in string format
		//List items = new ArrayList();
        //Tuple to return to PIG script
		Tuple returnTuple = TupleFactory.getInstance().newTuple();
		//Tuple nw_pmluTuple = TupleFactory.getInstance().newTuple();
		DataBag returnBag = BagFactory.getInstance().newDefaultBag();
		DataBag returnBagToScript = BagFactory.getInstance().newDefaultBag();

		//DataBag returnBagToScript = BagFactory.getInstance().newSortedBag();
		boolean pkinfo_ret=false;
		long pk_info_size = bagFromPigPackinfo.size();
	    //Iterator<Tuple> iter = bagFromPigPackinfo.iterator();
	    
	    //returnTuple = iter.next();
	    
		//Converting tuples to string and adding them to an arraylist
         if(pk_info_size ==1)
			{
			for (Tuple tuple : bagFromPigPackinfo)
			  {
			    returnTuple = tuple;
			     for(Tuple pmlu_tup: bagFromPigPmlu)
			        {
					    //25 is module list started in bag 2
					    for(int i=25;i<=121;i=i+2)
					    {  /*
					    	DataByteArray r_mod = (DataByteArray) pmlu_tup.get(3);
					    	DataByteArray a_mod = (DataByteArray) pmlu_tup.get(5);
					    	DataByteArray a_mod_dt = (DataByteArray) pmlu_tup.get(6);
					    	DataByteArray mod = (DataByteArray) returnTuple.get(i);
					    	Tuple req_field_pmlu = TupleFactory.getInstance().newTuple();
					    	req_field_pmlu.append(r_mod);
					    	req_field_pmlu.append(a_mod);
					    	req_field_pmlu.append(a_mod_dt);
					    	req_field_pmlu.append(mod);
					    	returnBag.add(req_field_pmlu);
					    	*/
			           	 if(pmlu_tup.get(3).toString().compareTo(returnTuple.get(i).toString()) ==0)
			           	   {
			           		//set 1 if pmlu is applying for this record.
			           		 //nw_pmluTuple.append("1");
			           		 returnTuple.set(i, pmlu_tup.get(5));
			           		 returnTuple.set(i+1, pmlu_tup.get(6));
			           		 returnBag.add(pmlu_tup);
			           		 pkinfo_ret =true;
			           		 break;
			    	       };
			    
			            }
			          }
				//items.add((String) tuple.get(0));
		      } 
	       }
		
               // Sorting the values
		//Collections.sort(items);

        //Putting the sorted values in Tuple format
		//for (String item : items) {
		//	returnTuple.append(item);
		//}
        //returnBagToScript(tuple:pkinfo, bag: pmlu applied tuples)
         if (pkinfo_ret) 
		 returnBagToScript.add(returnTuple);
		 
		 returnBagToScript.addAll(returnBag);
    
		 return returnBagToScript;
	}
	
	/*
	  public Schema outputSchema(Schema input) {
	         try{
	             Schema bagSchema = new Schema();
	             bagSchema.add(new Schema.FieldSchema("union_tuples", DataType.TUPLE));
	             //bagSchema.add(new Schema.FieldSchema("pmlu_applied_files", DataType.BAG));

	             return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
	                                                    bagSchema, DataType.BAG));
	         }catch (Exception e){
	            return null;
	         }finally{}
	         
	         
	  }  */
	  
}
	

/* next step match the blms using matches for  specifc feild in tuple, 
 * pmlu tuple will get split using split command and packinfo tuple get splitted.
 * the insert into packinfo by applying the same schema since no chnage in schema,
 * and use pmlu tuple to feed the cash processed tables.
 *  
 */


