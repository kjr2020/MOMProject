package MOM;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class MOMWorkload {

	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		PrintWriter pw = new PrintWriter("/Users/hyeockjin/RabbitData/sleeptask.txt");
		Random rand = new Random();
		
		for(int i = 0 ; i < 3000 ; i++) {
			String str = Integer.toString(rand.nextInt(101));
			System.out.println(str);
			pw.println(str);
		}
		pw.close();
	}

}