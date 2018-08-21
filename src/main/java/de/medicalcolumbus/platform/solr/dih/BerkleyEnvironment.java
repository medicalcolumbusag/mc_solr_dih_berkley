package de.medicalcolumbus.platform.solr.dih;

import com.sleepycat.je.*;

import java.io.File;

public class BerkleyEnvironment extends Environment {
	public BerkleyEnvironment(File envHome, EnvironmentConfig configuration) throws DatabaseException, IllegalArgumentException {
		super(envHome, configuration);
	}

	public void close(){
		while(envImpl.isValid()){
			envImpl.close();
		}
	}

}
