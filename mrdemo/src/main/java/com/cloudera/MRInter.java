package com.cloudera;

import java.util.Map;

import com.dinfo.app.utils.flow.DistributeComInputParam;


public interface MRInter {
	public Map<String,String> excute(String[] args);
}
