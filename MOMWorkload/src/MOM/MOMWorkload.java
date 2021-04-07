package MOM;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class MOMWorkload {

	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		PrintWriter pw = new PrintWriter("/Users/hyeockjin/RabbitData/sleeptask.txt");
		Random rand = new Random();
		int temp;
		String str;
		
		for(int i = 0 ; i < 3000 ; i++) {
			temp = rand.nextInt(100);
			if (temp/10 == 0)
				str="0"+Integer.toString(temp);
			else
				str=Integer.toString(temp);
			System.out.println(str);
			pw.println(str);
		}
		pw.close();
	}

}