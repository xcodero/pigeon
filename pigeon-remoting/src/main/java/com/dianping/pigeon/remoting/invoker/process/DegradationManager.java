/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker.process;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.dianping.pigeon.monitor.MonitorTransaction;
import com.dianping.pigeon.remoting.common.util.InvocationUtils;
import com.dianping.pigeon.remoting.invoker.exception.*;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import com.dianping.pigeon.config.ConfigChangeListener;
import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.monitor.Monitor;
import com.dianping.pigeon.monitor.MonitorLoader;
import com.dianping.pigeon.remoting.common.exception.RejectedException;
import com.dianping.pigeon.remoting.invoker.domain.InvokerContext;
import com.dianping.pigeon.remoting.invoker.process.filter.DegradationFilter;
import com.dianping.pigeon.remoting.invoker.route.quality.RequestQualityManager;
import com.dianping.pigeon.remoting.invoker.route.quality.RequestQualityManager.Quality;
import com.dianping.pigeon.threadpool.DefaultThreadFactory;

/**
 * @author xiangwu
 * 
 */
public enum DegradationManager {

	INSTANCE;

	private static final Logger logger = LoggerLoader.getLogger(DegradationManager.class);
	private static final ConfigManager configManager = ConfigManagerLoader.getConfigManager();
	private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Count>> requestSecondCountMap = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Count>>();
	private static volatile Map<String, Count> requestCountMap = null;
	private static final String KEY_DEGRADE_FORCE = "pigeon.invoker.degrade.force";
	private static final String KEY_DEGRADE_FAILURE = "pigeon.invoker.degrade.failure";
	private static final String KEY_DEGRADE_AUTO = "pigeon.invoker.degrade.auto";
	private static final String KEY_DEGRADE_RECOVER_PERCENT = "pigeon.invoker.degrade.recover.percent";
	private static final String KEY_DEGRADE_RECOVER_INTERVAL = "pigeon.invoker.degrade.recover.interval";
	private static final String KEY_DEGRADE_THRESHOLD_INVOKE = "pigeon.invoker.degrade.threshold.invoke";
	private static final String KEY_DEGRADE_THRESHOLD_TOTAL = "pigeon.invoker.degrade.threshold.total";
	private static final String KEY_DEGRADE_PERCENT_MAX = "pigeon.invoker.degrade.percent.max";
	private static final String KEY_DEGRADE_CHECK_SECONDS = "pigeon.invoker.degrade.check.seconds";
	private static final String KEY_DEGRADE_CHECK_INTERVAL = "pigeon.invoker.degrade.check.interval";
	private static final String KEY_DEGRADE_LOG_ENABLE = "pigeon.invoker.degrade.log.enable";
	private static final ExecutorService checkThreadPool = Executors.newFixedThreadPool(1,
			new DefaultThreadFactory("Pigeon-Client-Degrade-Checker"));
	private static final Random random = new Random();
	private final Monitor monitor = MonitorLoader.getMonitor();
	private static volatile boolean isForceDegrade = configManager.getBooleanValue(KEY_DEGRADE_FORCE, false);
	private static volatile boolean isAutoDegrade = configManager.getBooleanValue(KEY_DEGRADE_AUTO, false);
	private static volatile boolean isFailureDegrade = configManager.getBooleanValue(KEY_DEGRADE_FAILURE, false);
	private static volatile int degradeTotalThreshold = configManager.getIntValue(KEY_DEGRADE_THRESHOLD_TOTAL, 100);
	private static volatile int degradeInvokeThreshold = configManager.getIntValue(KEY_DEGRADE_THRESHOLD_INVOKE, 2);
	private static volatile float degradeRecoverPercent = configManager.getFloatValue(KEY_DEGRADE_RECOVER_PERCENT, 1);
	private static volatile int degradeRecoverInterval = configManager.getIntValue(KEY_DEGRADE_RECOVER_INTERVAL, 10);
	private static volatile float degradePercentMax = configManager.getFloatValue(KEY_DEGRADE_PERCENT_MAX, 99.90f);
	private static volatile int degradeCheckSeconds = configManager.getIntValue(KEY_DEGRADE_CHECK_SECONDS, 10);
	private static volatile int degradeCheckInterval = configManager.getIntValue(KEY_DEGRADE_CHECK_INTERVAL, 2);
	private static volatile boolean isLogDegrade = configManager.getBooleanValue(KEY_DEGRADE_LOG_ENABLE, false);

	private static final String KEY_DEGRADE_CUSTOMIZED_EXCEPTION = "pigeon.invoker.degrade.customized.exception";
	private static volatile Set<String> degradeCustomizedExceptions = new HashSet<>();

	DegradationManager() { }

	private static void parseDegradeCustomizedExceptions(String config) {
		Set<String> set = new HashSet<>();
		String[] exceptions = config.split(",");
		for (String ex : exceptions) {
			if (StringUtils.isNotBlank(ex)) {
				set.add(ex);
			}
		}
		degradeCustomizedExceptions = set;
	}

	public boolean isCustomizedDegradeException(Throwable throwable) {
		if (throwable != null) {
			return degradeCustomizedExceptions.contains(throwable.getClass().getName());
		}
		return false;
	}

	private static class InnerConfigChangeListener implements ConfigChangeListener {

		@Override
		public void onKeyUpdated(String key, String value) {
			try {
				if (key.endsWith(KEY_DEGRADE_FORCE)) {
					isForceDegrade = Boolean.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_AUTO)) {
					isAutoDegrade = Boolean.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_FAILURE)) {
					isFailureDegrade = Boolean.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_THRESHOLD_TOTAL)) {
					degradeTotalThreshold = Integer.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_THRESHOLD_INVOKE)) {
					degradeInvokeThreshold = Integer.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_RECOVER_PERCENT)) {
					degradeRecoverPercent = Float.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_RECOVER_INTERVAL)) {
					degradeRecoverInterval = Integer.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_PERCENT_MAX)) {
					degradePercentMax = Float.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_CHECK_SECONDS)) {
					degradeCheckSeconds = Integer.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_CHECK_INTERVAL)) {
					degradeCheckInterval = Integer.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_LOG_ENABLE)) {
					isLogDegrade = Boolean.valueOf(value);
				} else if (key.endsWith(KEY_DEGRADE_CUSTOMIZED_EXCEPTION)) {
					parseDegradeCustomizedExceptions(value);
				}
			} catch (RuntimeException e) {
				logger.warn("invalid value for key " + key, e);
			}
		}

		@Override
		public void onKeyAdded(String key, String value) {

		}

		@Override
		public void onKeyRemoved(String key) {

		}

	}

	static {
		// 启动定时任务
		checkThreadPool.execute(new Checker());
		// 读取配置
		String degradeCustomizedExceptionConfig = ConfigManagerLoader.getConfigManager()
				.getStringValue(KEY_DEGRADE_CUSTOMIZED_EXCEPTION, "");
		parseDegradeCustomizedExceptions(degradeCustomizedExceptionConfig);
		ConfigManagerLoader.getConfigManager().registerConfigChangeListener(new InnerConfigChangeListener());
	}

	public String getRequestUrl(InvokerContext context) {
		return context.getInvokerConfig().getUrl() + "#" + context.getMethodName();
	}

	public boolean needDegrade(InvokerContext context) {
		// 请求的服务方法开启降级
		if (degradationIsEnable(context)) {
			// CASE1:强制降级
			if (isForceDegrade) {
				return true;
			}

			// CASE2:自动降级
			if (isAutoDegrade) {
				if (!CollectionUtils.isEmpty(requestCountMap)) {
					// 查询服务方法的请求情况统计值
					String requestUrl = getRequestUrl(context);
					Count count = requestCountMap.get(requestUrl);
					if (count != null) {
						// 条件1：总请求数>=100 【小流量情形】
						if (count.getTotalValue() >= degradeTotalThreshold) {
							// 条件2：真正发到服务端的请求数>2 AND 这些请求的失败率<1% 【失败率低的情形】
							if ((count.getTotalValue() - count.getDegradedValue()) > degradeInvokeThreshold
									&& count.getFailedPercent() < degradeRecoverPercent) {
								/*** 减少需要降级的请求数（相对当前降级率而言，少10%）***/
								return random(count.getDegradedPercent() - degradeRecoverInterval);
								// 条件3：失败率>=1%  【失败率高的情形】
							} else if (count.getFailedPercent() >= degradeRecoverPercent) {
								/*** 将99.9%的请求进行降级 ***/
								return random(degradePercentMax);
							}
						}
					}
				}
			}

			// CASE3:失败降级
			if (isFailureDegrade) {
				return false;
			}
		}
		return false;
	}

	public static Map<String, Count> getRequestCountMap() {
		return requestCountMap;
	}

	public boolean needFailureDegrade(InvokerContext context) {
		return degradationIsEnable(context)
				&& (isAutoDegrade || isFailureDegrade);
	}

	private boolean degradationIsEnable(InvokerContext context) {
		DegradationFilter.DegradeAction action = DegradationFilter.getDegradeMethodActions()
				.get(getRequestUrl(context));
		return action != null && action.getEnable();
	}

	private boolean random(float percent) {
		return random.nextInt(10000) < percent * 100;
	}

	public void addFailedRequest(InvokerContext context, Throwable t) {
		if (t instanceof ServiceUnavailableException || t instanceof RequestTimeoutException
				|| t instanceof RemoteInvocationException || t instanceof RejectedException
				|| t instanceof ServiceFailureDegreadedException || isCustomizedDegradeException(t)) {
			addRequest(context, t, false);
		}
	}

	public void addDegradedRequest(InvokerContext context, Throwable t) {
		addRequest(context, null, true);
		if (isLogDegrade && !(t instanceof ServiceDegradedException)) {
			ServiceDegradedException ex = new ServiceDegradedException(getRequestUrl(context), t);
			ex.setStackTrace(new StackTraceElement[] {});
			monitor.logError(ex);
		}
	}

	public void addNormalRequest(InvokerContext context) {
		addRequest(context, null, false);
	}

	private void addRequest(InvokerContext context, Throwable t, boolean degraded) {
		if (isAutoDegrade || isForceDegrade) {
			int currentSecond = Calendar.getInstance().get(Calendar.SECOND);
			String requestUrl = getRequestUrl(context);
			ConcurrentHashMap<Integer, Count> secondCount = requestSecondCountMap.get(requestUrl);
			if (secondCount == null) {
				secondCount = new ConcurrentHashMap<Integer, Count>();
				ConcurrentHashMap<Integer, Count> last = requestSecondCountMap.putIfAbsent(requestUrl, secondCount);
				if (last != null) {
					secondCount = last;
				}
			}
			Count count = secondCount.get(currentSecond);
			if (count == null) {
				count = new Count(0, 0, 0);
				Count last = secondCount.putIfAbsent(currentSecond, count);
				if (last != null) {
					count = last;
				}
			}
			count.total.incrementAndGet();
			if (t != null) {
				count.failed.incrementAndGet();
			}
			if (degraded) {
				count.degraded.incrementAndGet();
			}
		}
	}

	public void monitorDegrade(InvokerContext context, MonitorTransaction transaction) {
		String callInterface = InvocationUtils.getRemoteCallFullName(context.getInvokerConfig().getUrl(),
				context.getMethodName(), context.getParameterTypes());

		if (context.getDegradeInfo().isDegrade()
				&& !context.getDegradeInfo().isFailureDegrade()) {
			transaction.logEvent("PigeonCall.degrade", callInterface, "");
		} else if (context.getDegradeInfo().isDegrade()
				&& context.getDegradeInfo().isFailureDegrade()) {
			transaction.logEvent("PigeonCall.failureDegrade", callInterface,
					context.getDegradeInfo().getCause() == null ?
							"" : context.getDegradeInfo().getCause().toString());
		}
	}

	public static class DegradeActionConfig implements Serializable {

		private static final long serialVersionUID = 1L;

		private String returnClass;
		private String componentClass;
		private String keyClass;
		private String valueClass;
		private String content;
		private boolean throwException = false;
		private boolean useMockClass = false;
		private boolean useGroovyScript = false;
		private boolean enable = true;

		public boolean getEnable() {
			return enable;
		}

		public void setEnable(boolean enable) {
			this.enable = enable;
		}

		public boolean getUseGroovyScript() {
			return useGroovyScript;
		}

		public void setUseGroovyScript(boolean useGroovyScript) {
			this.useGroovyScript = useGroovyScript;
		}

		public boolean getUseMockClass() {
			return useMockClass;
		}

		public void setUseMockClass(boolean useMockClass) {
			this.useMockClass = useMockClass;
		}

		public boolean getThrowException() {
			return throwException;
		}

		public void setThrowException(boolean throwException) {
			this.throwException = throwException;
		}

		public DegradeActionConfig() {
		}

		public String getReturnClass() {
			return returnClass;
		}

		public void setReturnClass(String returnClass) {
			this.returnClass = returnClass;
		}

		public String getComponentClass() {
			return componentClass;
		}

		public void setComponentClass(String componentClass) {
			this.componentClass = componentClass;
		}

		public String getKeyClass() {
			return keyClass;
		}

		public void setKeyClass(String keyClass) {
			this.keyClass = keyClass;
		}

		public String getValueClass() {
			return valueClass;
		}

		public void setValueClass(String valueClass) {
			this.valueClass = valueClass;
		}

		public String getContent() {
			return content;
		}

		public void setContent(String content) {
			this.content = content;
		}

	}

	public static class Count {
		private AtomicInteger failed = new AtomicInteger();
		private AtomicInteger total = new AtomicInteger();
		private AtomicInteger degraded = new AtomicInteger();

		public Count() {
		}

		public Count(int total, int failed, int degraded) {
			this.total.set(total);
			this.failed.set(failed);
			this.degraded.set(degraded);
		}

		public AtomicInteger getFailed() {
			return failed;
		}

		public int getFailedValue() {
			return failed.get();
		}

		public void setFailed(int failed) {
			this.failed.set(failed);
		}

		public AtomicInteger getDegraded() {
			return degraded;
		}

		public int getDegradedValue() {
			return degraded.get();
		}

		public void setDegraded(int degraded) {
			this.degraded.set(degraded);
		}

		public AtomicInteger getTotal() {
			return total;
		}

		public int getTotalValue() {
			return total.get();
		}

		public void setTotal(int total) {
			this.total.set(total);
		}

		public Count merge(Count count) {
			Count n = new Count();
			n.total.set(this.total.get() + count.total.get());
			n.failed.set(this.failed.get() + count.failed.get());
			n.degraded.set(this.degraded.get() + count.degraded.get());
			return n;
		}

		public float getFailedPercent() {
			int m = (total.get() - degraded.get());
			if (total.get() > 0 && m > 0) {
				return failed.get() * 100 / m;
			} else {
				return 0;
			}
		}

		public float getDegradedPercent() {
			if (total.get() > 0) {
				return degraded.get() * 100 / total.get();
			} else {
				return 0;
			}
		}

		public void clear() {
			total.set(0);
			failed.set(0);
			degraded.set(0);
		}
	}

	static class Checker implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(1000 * degradeCheckInterval);
					checkRequestSecondCount();
				} catch (Exception e) {
					logger.error("", e);
				}
			}
		}

		private void checkRequestSecondCount() {
			Map<String, Count> countMap = new ConcurrentHashMap<String, Count>();
			// 自动降级时，统计的时间窗口，默认10s
			final int recentSeconds = degradeCheckSeconds;
			final int currentSecond = Calendar.getInstance().get(Calendar.SECOND);

			for (String url : requestSecondCountMap.keySet()) {
				Map<Integer, Count> secondCount = requestSecondCountMap.get(url);
				int total = 0, failed = 0, degraded = 0;
				for (int i = 1; i <= recentSeconds; i++) {
					int prevSec = currentSecond - i;
					prevSec = prevSec >= 0 ? prevSec : prevSec + 60;
					Count ct = secondCount.get(prevSec);
					if (ct != null) {
						// 将时间窗口内的每秒计数值进行聚合
						total += ct.getTotalValue();
						failed += ct.getFailedValue();
						degraded += ct.getDegradedValue();
					}
				}
				// 放入聚合结果Map
				countMap.put(url, new Count(total, failed, degraded));
				// 重置requestSecondCountMap中的秒级计数器
				for (int i = recentSeconds + 1; i <= recentSeconds + 20; i++) {
					int prevSec = currentSecond - i;
					prevSec = prevSec >= 0 ? prevSec : prevSec + 60;
					Count ct = secondCount.get(prevSec);
					if (ct != null) {
						ct.clear();
					}
				}
			}
			Map<String, Count> old = requestCountMap;
			requestCountMap = countMap;
			if (old != null) {
				old.clear();
				old = null;
			}

			// 复用降级统计和清空的线程，用于服务质量统计和清空（窗口默认为10秒）
			ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer, Quality>>> addrReqUrlSecondQualities = RequestQualityManager.INSTANCE
					.getAddrReqUrlSecondQualities();
			ConcurrentMap<String, ConcurrentMap<String, Quality>> addrReqUrlQualities = new ConcurrentHashMap<String, ConcurrentMap<String, Quality>>();

			for (String address : addrReqUrlSecondQualities.keySet()) {
				ConcurrentMap<String, ConcurrentMap<Integer, Quality>> reqUrlSecondQualities = addrReqUrlSecondQualities
						.get(address);

				ConcurrentHashMap<String, Quality> reqUrlQualities = new ConcurrentHashMap<String, Quality>();
				for (String requestUrl : reqUrlSecondQualities.keySet()) {
					ConcurrentMap<Integer, Quality> secondQualities = reqUrlSecondQualities.get(requestUrl);
					int total = 0, failed = 0;
					for (int i = 1; i <= recentSeconds; i++) {
						int prevSec = currentSecond - i;
						prevSec = prevSec >= 0 ? prevSec : prevSec + 60;
						Quality quality = secondQualities.get(prevSec);

						if (quality != null) {
							total += quality.getTotalValue();
							failed += quality.getFailedValue();
						}
					}

					reqUrlQualities.put(requestUrl, new Quality(total, failed));

					// clear previous seconds
					for (int i = recentSeconds + 1; i <= recentSeconds + 20; i++) {
						int prevSec = currentSecond - i;
						prevSec = prevSec >= 0 ? prevSec : prevSec + 60;
						Quality quality = secondQualities.get(prevSec);

						if (quality != null) {
							quality.clear();
						}
					}
				}

				addrReqUrlQualities.put(address, reqUrlQualities);
			}

			RequestQualityManager.INSTANCE.setAddrReqUrlQualities(addrReqUrlQualities);
		}

	}
}
