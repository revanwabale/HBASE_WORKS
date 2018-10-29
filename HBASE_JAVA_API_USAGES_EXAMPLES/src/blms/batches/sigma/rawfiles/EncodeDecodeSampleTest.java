package blms.batches.sigma.rawfiles;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import javax.xml.bind.DatatypeConverter;

/**
 * @author fjt02336
 * 
 */
public class EncodeDecodeSampleTest {
	
	public static void main(String args[]) throws UnsupportedEncodingException, FileNotFoundException {
		//Example String For encoding
		//String RawfileString = "RowsNo-1: hello world\n RowsNo-2:\t after 1 space\tafter 2nd space        after blankspaces\nRowsNo-3!@#$%^&*()AfterWeiredCharacters\nLastRow" ;
		
	//	String RawfileString = "38324A4E314641415A4530553030303932303132393341306183334E41304350420517000000000096060000000000000080201409101656210061010000020D02870000049DFFFFFFFF12862AF8921A3850038C005C201E00065789000896E18000016162028B00040E0F00250000000000000000000000000270000C0000000000000000000000000E09002A000000000000000000000000026800140000000000000000000000000018005B00C8019501D70260030E041C0015004200440081009C0048003400480000000000000000000000000000018161843233304A543131314233303030323835202020206161201E21850C2147218503";
	//	System.out.println("\nString For Encoding & Decoding: "+RawfileString);
		
	//	byte[] message = RawfileString.getBytes("UTF-8");
	//	byte[] message ="UEsDBBQAAAAIAOxicEsRzvahVQAAAG4AAABVAAAAVklOKExHQkY1QUUwMUVSMTMyMjM3KV8yMDE3MTExNjEyMTIyNV80X1BSQ19fX19fX19fX19fX19fX19fNEpLU0E5OTM3Nl9fX0wzM19fX18xLm11eGWJMQ6AIBAEMTH0UmgrhT0cGmzPBLSw4ge8RKMP8Ml4amWY3Ww2mdGu8+QHdEq7oA2AsVSt4tnjopAhL9mPvW2qWoiuYBzktUkRj+DTYxLx7fsoGTdQSwECFAAUAAAACADsYnBLEc72oVUAAABuAAAAVQAAAAAAAAAAACAAAAAAAAAAVklOKExHQkY1QUUwMUVSMTMyMjM3KV8yMDE3MTExNjEyMTIyNV80X1BSQ19fX19fX19fX19fX19fX19fNEpLU0E5OTM3Nl9fX0wzM19fX18xLm11eFBLBQYAAAAAAQABAIMAAADIAAAAAAA=".getBytes("UTF-8");
				
		//String encoded_value = DatatypeConverter.printBase64Binary(message);
		String encoded_value = "UEsDBBQAAQAIAJxpRky507qLKwEAAFUCAABVAAAAVklOKExHQkY1QUUwMEZSMzI3NTU0KV8yMDE4MDIwNjEyNTQxN180X1BSQ19fX19fX19fX19fX19fX19fN0JLU0E4MDk3NV9fX0wzM19fX185LmhzdFlarhEZFp0Cqp9aWM7MXMVqNHIs1s3uplKVQUXxj1FL+Ql2pNybOPEE/TMbBLJz2RDjaaN/bdtsdJnxy0JX5WhpPjV8ZFvckO6Hbq4YJs4QB4Se9gHEYKsVvswRApG8mrV5W8UWjX4CbGkI6uXOqzYpMOcxAweUH6C/cqxmoX4XJZGvfrzLTqpkgnQUyCm4AI8CUlI1y3M/ETkosU8VEbyqW0f9NpCxj0ZzHgcHRu/lbj2dEz0UXk8XE23cJsEc5zP//K6zhKJskU9EG5805nL+yE5zOJjzlJSkRQ6/3tdq262fgimw/aSFyBUZLII/YADBADLS6PYGhhkndkCZhoEap1PKGfw+0w/yIEV8ySA10q4oVVAT5AO1DRonpWCLhkBPBiAzyPhIHyZ+UEsBAhQAFAABAAgAnGlGTLnTuosrAQAAVQIAAFUAAAAAAAAAAAAgAAAAAAAAAFZJTihMR0JGNUFFMDBGUjMyNzU1NClfMjAxODAyMDYxMjU0MTdfNF9QUkNfX19fX19fX19fX19fX19fXzdCS1NBODA5NzVfX19MMzNfX19fOS5oc3RQSwUGAAAAAAEAAQCDAAAAngEAAAAA";
		System.out.println("\nEncoded Value:" + encoded_value);
		String decoded_value = new String(DatatypeConverter.parseBase64Binary(encoded_value));
		System.out.println("\nDecoded Value:" + decoded_value);
		System.out.println("\n parse for binary content: ");

		PrintWriter writer = new PrintWriter("DecodedFile.txt", "UTF-8");
		writer.println(decoded_value);
		writer.close();	
		//byte[] message = decoded_value.getBytes("UTF-8");
		
		String HexString = String.format("%21X", Long.parseLong(decoded_value,2)) ;
		System.out.println("\nHexString:" + HexString);
		

		/*	
		int decimal = Integer.parseInt(decoded_value,2);
		String hexStr = Integer.toString(decimal,16);
		System.out.println("\nDecoded Value:" + hexStr);
			
		for(int i = 0 ; i< message.length;i++)
		System.out.println(message[i]);
		if(decoded_value.equals(RawfileString));
		System.out.println("\nEncode and decoded Strings are matching..! Bingo"); */
		System.gc();
	
		}

}
