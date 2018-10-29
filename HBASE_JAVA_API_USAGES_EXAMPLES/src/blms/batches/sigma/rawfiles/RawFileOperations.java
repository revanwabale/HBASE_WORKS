package blms.batches.sigma.rawfiles;

import java.io.UnsupportedEncodingException;
import javax.xml.bind.DatatypeConverter;

/**
 * @author fjt02336
 * 
 */
public class RawFileOperations {
	
	byte[] rawfile = null;
	String encoded = null;
	String decoded = null;
	

	public RawFileOperations(byte[] message) {
		rawfile = message;
	}
	
	/**
	 * @return
	 */
	public String encodeBase64() {

		encoded = DatatypeConverter.printBase64Binary(rawfile);
		//System.out.println(encoded);
		return(encoded);
		}
	
	public String decodeBase64() {
		
		decoded = new String(DatatypeConverter.parseBase64Binary(encoded));
		//System.out.println(decoded);
			return(decoded);
	}
	
	
	/**
	 * @param args
	 * @throws UnsupportedEncodingException
	 */
	public static void main(String args[]) throws UnsupportedEncodingException {
		//Example String For encoding
		//String RawfileString = "RowsNo-1: hello world\n RowsNo-2:\t after 1 space\tafter 2nd space        after blankspaces\nRowsNo-3!@#$%^&*()AfterWeiredCharacters\nLastRow" ;
		
		String RawfileString = "38324A4E314641415A4530553030303932303132393341306183334E41304350420517000000000096060000000000000080201409101656210061010000020D02870000049DFFFFFFFF12862AF8921A3850038C005C201E00065789000896E18000016162028B00040E0F00250000000000000000000000000270000C0000000000000000000000000E09002A000000000000000000000000026800140000000000000000000000000018005B00C8019501D70260030E041C0015004200440081009C0048003400480000000000000000000000000000018161843233304A543131314233303030323835202020206161201E21850C2147218503";
		System.out.println("\nString For Encoding & Decoding: "+RawfileString);
		
		byte[] message = RawfileString.getBytes("UTF-8");
		RawFileOperations RawFile1 = new RawFileOperations(message);
		
		String encoded_value = RawFile1.encodeBase64();
		System.out.println("\nEncoded Value:" + encoded_value);
		
		String decoded_value = RawFile1.decodeBase64();
		System.out.println("\nDecoded Value:" + decoded_value);
		
		if(decoded_value.equals(RawfileString));
		System.out.println("\nEncode and decoded Strings are matching..! Bingo");
		System.gc();
	
		}

}
