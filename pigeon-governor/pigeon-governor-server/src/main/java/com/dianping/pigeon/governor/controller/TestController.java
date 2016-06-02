package com.dianping.pigeon.governor.controller;

import com.dianping.pigeon.governor.bean.Result;
import com.dianping.pigeon.governor.bean.ServiceWithGroup;
import com.dianping.pigeon.governor.exception.DbException;
import com.dianping.pigeon.governor.model.Service;
import com.dianping.pigeon.governor.service.ServiceService;
import com.dianping.pigeon.governor.task.CheckAndSyncServiceDB;
import com.dianping.pigeon.governor.util.IPUtils;
import com.dianping.pigeon.governor.util.OpType;
import com.dianping.pigeon.registry.RegistryManager;
import com.dianping.pigeon.registry.zookeeper.CuratorClient;
import com.dianping.pigeon.registry.zookeeper.CuratorRegistry;
import com.dianping.pigeon.registry.zookeeper.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

/**
 * Created by chenchongze on 16/1/26.
 */
@Controller
@RequestMapping("/test")
public class TestController {

    private Logger logger = LogManager.getLogger();
    @Autowired
    private CheckAndSyncServiceDB checkAndSyncServiceDB;
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;
    @Autowired
    private ServiceService serviceService;

    private Map<ServiceWithGroup, Service> serviceGroupDbIndex = CheckAndSyncServiceDB.getServiceGroupDbIndex();

    private CuratorClient client;

    public TestController() {
        CuratorRegistry registry = (CuratorRegistry) RegistryManager.getInstance().getRegistry();
        client =  registry.getCuratorClient();
    }

    @RequestMapping(value = "/betaonly/dellocalip", method = {RequestMethod.POST})
    @ResponseBody
    public Result dellocalip(@RequestParam(value="validate") final String validate) {

        if(IPUtils.getFirstNoLoopbackIP4Address().equalsIgnoreCase(validate)) {

            threadPoolTaskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        checkAndSyncServiceDB.loadFromDb();
                    } catch (DbException e1) {
                        logger.warn("load from db failed!try again!",e1);
                        try {
                            checkAndSyncServiceDB.loadFromDb();
                        } catch (DbException e2) {
                            logger.error("load from db failed!!",e2);
                        }
                    }

                    serviceGroupDbIndex = CheckAndSyncServiceDB.getServiceGroupDbIndex();

                    for (ServiceWithGroup serviceWithGroup : serviceGroupDbIndex.keySet()) {
                        final Service serviceDb = serviceGroupDbIndex.get(serviceWithGroup);
                        String hosts = serviceDb.getHosts();
                        if(StringUtils.isNotBlank(hosts) && StringUtils.isBlank(serviceDb.getGroup())) { // 默认泳道有机器

                            Set<String> hostSet = new HashSet<String>();
                            hostSet.addAll(Arrays.asList(hosts.split(",")));
                            boolean needUpdate = false;

                            for(String host : hosts.split(",")) {
                                if(!host.startsWith("192.168") && !host.startsWith("10.66")) {
                                    hostSet.remove(host);
                                    needUpdate = true;
                                }
                            }

                            // 更新数据库和zk
                            if(needUpdate) {
                                String newHostList = StringUtils.join(hostSet, ",");
                                String service_zk = Utils.escapeServiceName(serviceWithGroup.getService());
                                String serviceHostAddress = "/DP/SERVER/" + service_zk;

                                try {
                                    client.set(serviceHostAddress, newHostList);
                                } catch (Exception e) {
                                    logger.error("write zk error! return!", e);
                                    return;
                                }

                                //update database
                                serviceDb.setHosts(newHostList);
                                serviceService.updateById(serviceDb);

                                logger.warn("update: " + serviceWithGroup + " with: " + newHostList);
                            }
                        }
                    }

                }
            });

            return Result.createSuccessResult("start job...");

        } else {

            return Result.createErrorResult("failed to validate...");

        }

    }

    @RequestMapping(value = {"/syncdb"}, method = {RequestMethod.POST})
    @ResponseBody
    public Result syncdb(@RequestParam(value="validate") final String validate,
                              HttpServletRequest request, HttpServletResponse response) {
        if(IPUtils.getFirstNoLoopbackIP4Address().equalsIgnoreCase(validate)) {
            threadPoolTaskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    checkAndSyncServiceDB.checkAndSyncDB();
                }
            });
            return Result.createSuccessResult("start sync db...");
        } else {
            return Result.createErrorResult("failed to validate...");
        }

    }

    @RequestMapping(value = {"/loglevel"}, method = {RequestMethod.GET})
    @ResponseBody
    public Result loglevel(HttpServletRequest request, HttpServletResponse response) {
        logger.trace("trace");
        logger.debug("debug");
        logger.info("info");
        logger.warn("warn");
        logger.error("error");
        logger.fatal("fatal");

        return Result.createSuccessResult("success!");
    }

}