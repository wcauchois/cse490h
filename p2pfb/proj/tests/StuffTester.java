package tests;
import java.util.regex.*;

import java.util.*;

import edu.washington.cs.cse490h.lib.Utility;

import main.AppUtil;
public class StuffTester {
    
	public static void main(String[] args) {
		//System.out.println(Arrays.toString(AppUtil.splitArgs("get foo.txt ", 3)));
	    Pattern foo = Pattern.compile("(.+?)\\((\\d+)\\)");
	    Matcher m = foo.matcher("foo.txt(3)");
	    m.matches();
	    System.out.println(m.group(0));
	    System.out.println(m.group(1));
	    System.out.println(m.group(2));
	}
}
