package com.datacenter.hbase.Util;

import java.util.LinkedHashMap;
import java.util.Map;
/**
 * LRU cache which is simplest implements 
 * reference http://svn.apache.org/repos/asf/poi/trunk/src/examples/src/org/apache/poi/xssf/eventusermodel/examples/FromHowTo.java
 * @author Administrator
 * @param <A>
 * @param <B>
 */
public class LruCache<A,B> extends LinkedHashMap<A, B> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final int maxEntries;

    public LruCache(final int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
        return super.size() > maxEntries;
    }
}
