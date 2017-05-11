package com.qiniu.android.common;

import android.util.Log;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by long on 2016/9/29.
 */

public final class FixedZone extends Zone {
    /**
     * 华东机房
     */
    public static final Zone zone0 = new FixedZone(new String[]{
            "proxy-upload-qiniup-com.qnssl.com", "proxy-upload-nb-qiniup-com.qnssl.com",
            "proxy-upload-xs-qiniup-com.qnssl.com", "proxy-up-qiniup-com.qnssl.com",
            "proxy-up-nb-qiniup-com.qnssl.com", "proxy-up-xs-qiniup-com.qnssl.com",
            "proxy-upload-qbox-me.qnssl.com", "proxy-up-qbox-me.qnssl.com"
    });

    /**
     * 华北机房
     */
    public static final Zone zone1 = new FixedZone(new String[]{
            "proxy-upload-z1-qiniup-com.qnssl.com", "proxy-up-z1-qiniup-com.qnssl.com",
            "upload-z1-qbox-me.qnssl.com", "proxy-up-z1-qbox-me.qnssl.com"
    });

    /**
     * 华南机房
     */
    public static final Zone zone2 = new FixedZone(new String[]{
            "proxy-upload-z2-qiniup-com.qnssl.com", "proxy-upload-gz-qiniup-com.qnssl.com",
            "upload-fs-qiniup-com.qnssl.com", "proxy-up-z2-qiniup-com.qnssl.com",
            "up-gz-qiniup-com.qnssl.com", "proxy-up-fs-qiniup-com.qnssl.com",
            "upload-z2-qbox-me.qnssl.com", "proxy-up-z2-qbox-me.qnssl.com"
    });

    /**
     * 北美机房
     */
    public static final Zone zoneNa0 = new FixedZone(new String[]{
            "proxy-upload-na0-qiniu-com.qnssl.com", "proxy-up-na0-qiniup-com.qnssl.com",
            "proxy-upload-na0-qbox-me.qnssl.com", "proxy-up-na0-qbox-me.qnssl.com"
    });

    private ZoneInfo zoneInfo;

    public FixedZone(ZoneInfo zoneInfo) {
        this.zoneInfo = zoneInfo;
    }

    public FixedZone(String[] upDomains) {
        this.zoneInfo = createZoneInfo(upDomains);
    }

    public static ZoneInfo createZoneInfo(String[] upDomains) {
        List<String> upDomainsList = new ArrayList<String>();
        Map<String, Long> upDomainsMap = new ConcurrentHashMap<String, Long>();
        for (String domain : upDomains) {
            upDomainsList.add(domain);
            upDomainsMap.put(domain, 0L);
        }
        return new ZoneInfo(0, upDomainsList, upDomainsMap);
    }

    @Override
    public synchronized String upHost(String upToken, boolean useHttps, String frozenDomain) {
        String upHost = this.upHost(this.zoneInfo, useHttps, frozenDomain);
        Log.d("Qiniu.FixedZone", upHost + ", " + this.zoneInfo.upDomainsMap);
        return upHost;
    }

    @Override
    public void preQuery(String token, QueryHandler complete) {
        complete.onSuccess();
    }

    @Override
    public boolean preQuery(String token) {
        return true;
    }

    @Override
    public synchronized void frozenDomain(String upHostUrl) {
        if (upHostUrl != null) {
            URI uri = URI.create(upHostUrl);
            //frozen domain
            String frozenDomain = uri.getHost();
            zoneInfo.frozenDomain(frozenDomain);
        }
    }
}
