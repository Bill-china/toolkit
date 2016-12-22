package com.qihoo.toolkit.db;
import redis.clients.jedis.Jedis;

public class RedisUtil {
	private Jedis redis = null;
	//删除key
	public Jedis redisConn(String rhost, int rport) {
		redis = new Jedis (rhost, rport);//连接redis 
		return redis;
	}
	
	public void keyDel(String key) {
		redis.del("name1");
	}
}
