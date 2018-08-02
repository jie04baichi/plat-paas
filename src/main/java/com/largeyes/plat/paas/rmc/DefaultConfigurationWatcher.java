package com.largeyes.plat.paas.rmc;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DefaultConfigurationWatcher  implements Watcher{

	@Override
	public void process(WatchedEvent event) {		
	    //System.out.println("This path config is change. PATH:"+event.getPath());
	}

}
