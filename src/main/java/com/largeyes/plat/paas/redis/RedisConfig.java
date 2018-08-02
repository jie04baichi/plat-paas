package com.largeyes.plat.paas.redis;

public class RedisConfig {
	public static final String HOSTS_KEY = "hosts.list";
	public static final String TIMEOUT_KEY = "timeOut";
	public static final String MAXACTIVE_KEY = "maxActive";
	public static final String MAXIDLE_KEY = "maxIdle";
	public static final String MAXWAIT_KEY = "maxWait";
	public static final String TESTONBORROW_KEY = "testOnBorrow";
	public static final String TESTONRETURN_KEY = "testOnReturn";
	public static final String TIMEBETWEENEVICTIONRUNSMILLIS_KEY="timeBetweenEvictionRunsMillis";
	public static final String NUMTESTSPEREVICTIONRUN_KEY="numTestsPerEvictionRun";
	public static final String MINEVICTABLEIDLETIMEMILLIS_KEY="minEvictableIdleTimeMillis";
	public static final String TESTWHILEIDLE_KEY="testWhileIdle";
	public static final String SOFTMINEVICTABLEIDLETIMEMILLIS_KEY="softMinEvictableIdleTimeMillis";
	
	public static final String DBINDEX_KEY = "dbIndex";
	
	public static final String CONFIG_PATH = "/config/plat/paas/cache/redis";

}
