package lxfree.query3.decipher;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * read ROT13 version of banned words and decipher to get the original text
 * @author Jing XU
 *
 */

public class Decipher {

	private static void decipher(){
		/* the file storing deciphered words */
		File originalFile = new File("banned_words");
		/* the ROT13 version of the banned words */
		InputStream ROT13File = Decipher.class.getResourceAsStream("/ROT13_banned_words");
		BufferedReader ROT13br=new BufferedReader(new InputStreamReader(ROT13File, StandardCharsets.UTF_8));
		String line = new String();
		try {
			PrintWriter printWriter = new PrintWriter(originalFile, "UTF-8");
			line=ROT13br.readLine();
			while(line!=null){
				StringBuilder builder = new StringBuilder();
				int wordLength=line.length();
				for(int i=0;i<wordLength;i++){
					char ch=line.charAt(i);
					if(ch<65 || (ch>90 && ch<97) || ch>122){
						// if the character is not a letter
						builder.append(ch);
					}else if(ch>=65 && ch<=90){
						// if the character is upper class
						char chROT=(char)(ch+13);
						if(chROT>90)
							chROT-=26;
						builder.append(chROT);
					}else{
						// if the character is lower class
						char chROT=(char)(ch+13);
						if(chROT>122)
							chROT-=26;
						builder.append(chROT);
					}
				}
				printWriter.write(builder.toString()+System.lineSeparator());
				line=ROT13br.readLine();
			}
			ROT13br.close();
			printWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		decipher();
	}
}
