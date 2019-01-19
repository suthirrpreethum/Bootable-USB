package com.suthirr.mapReduce;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import com.opencsv.CSVReader;

public class SampleChck {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String str="07/01/2012,BRONX,10451,1,0,0,0,0,0,1,0,PASSENGER VEHICLE	__";
//		String[] s1=str.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
//		System.out.println(s1[11].substring(0, s1[11].length()-3)+"hi");
		//List<String> s=Arrays.asList(s1[0].split("/"));
		//s1.forEach(st->System.out.println(st));
		//System.out.println(s1[0].split("/")[2]);
		try {
			FileReader f=new FileReader("part.csv");
			CSVReader r=new CSVReader(f);
			String[] arr=r.readNext();
			r.close();
			for(int i=0;i<arr.length;i++)
				System.out.println(arr[i]);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
