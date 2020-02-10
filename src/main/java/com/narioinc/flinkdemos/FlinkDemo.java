package com.narioinc.flinkdemos;

import com.narioinc.flinkdemos.models.Rule;

public class FlinkDemo {
	private static Rule rule = new Rule("A", 2, 60000);
	private static FlinkAppManager manager = new FlinkAppManager(rule);
	
	public static void main(String args[]) {		
		manager.start();
	}
}
