package com.xxl.conf.admin.service.impl;

import com.xxl.conf.admin.core.model.*;
import com.xxl.conf.admin.core.util.RegexUtil;
import com.xxl.conf.admin.core.util.ReturnT;
import com.xxl.conf.admin.dao.*;
import com.xxl.conf.admin.service.IXxlConfNodeService;
import com.xxl.conf.core.util.PropUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;

import javax.annotation.Resource;
import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 配置
 *
 * @author xuxueli 2016-08-15 22:53
 */
@Service
public class XxlConfNodeServiceImpl implements IXxlConfNodeService, InitializingBean, DisposableBean {
	private static Logger logger = LoggerFactory.getLogger(XxlConfNodeServiceImpl.class);


	@Resource
	private XxlConfNodeDao xxlConfNodeDao;
	@Resource
	private XxlConfProjectDao xxlConfProjectDao;
	/*@Resource
	private XxlConfZKManager xxlConfZKManager;*/
	@Resource
	private XxlConfNodeLogDao xxlConfNodeLogDao;
	@Resource
	private XxlConfEnvDao xxlConfEnvDao;
	@Resource
	private XxlConfNodeMsgDao xxlConfNodeMsgDao;


	@Value("${xxl.conf.confdata.filepath}")
	private String confDataFilePath;
	@Value("${xxl.conf.access.token}")
	private String accessToken;

	private int confBeatTime = 30;
	private ExecutorService executorService = Executors.newCachedThreadPool();
	private volatile boolean executorStoped = false;
	private volatile List<Integer> readedMessageIds = Collections.synchronizedList(new ArrayList<Integer>());
	private Map<String, List<DeferredResult>> confDeferredResultMap = new ConcurrentHashMap<>();

	@Override
	public boolean ifHasProjectPermission(XxlConfUser loginUser, String loginEnv, String appname) {
		if (loginUser.getPermission() == 1) {
			return true;
		}
		if (ArrayUtils.contains(StringUtils.split(loginUser.getPermissionData(), ","), (appname.concat("#").concat(loginEnv)))) {
			return true;
		}
		return false;
	}

	/**
	 * 找到若干个=appname,=loginEnv,like %key% 的 key 和 value 等信息
	 */
	@Override
	public Map<String, Object> pageList(int offset,
										int pagesize,
										String appname,
										String key,
										XxlConfUser loginUser,
										String loginEnv) {

		// project permission
		if (StringUtils.isBlank(loginEnv) || StringUtils.isBlank(appname) || !ifHasProjectPermission(loginUser, loginEnv, appname)) {
			//return new ReturnT<String>(500, "您没有该项目的配置权限,请联系管理员开通");
			Map<String, Object> emptyMap = new HashMap<String, Object>();
			emptyMap.put("data", new ArrayList<>());
			emptyMap.put("recordsTotal", 0);
			emptyMap.put("recordsFiltered", 0);
			return emptyMap;
		}

		// xxlConfNode in mysql
		List<XxlConfNode> data = xxlConfNodeDao.pageList(offset, pagesize, loginEnv, appname, key);
		int list_count = xxlConfNodeDao.pageListCount(offset, pagesize, loginEnv, appname, key);

		// fill value in zk
		/*if (CollectionUtils.isNotEmpty(data)) {
			for (XxlConfNode node: data) {
				String realNodeValue = xxlConfZKManager.get(node.getEnv(), node.getKey());
				node.setZkValue(realNodeValue);
			}
		}*/

		// package result
		Map<String, Object> maps = new HashMap<String, Object>();
		maps.put("data", data);
		maps.put("recordsTotal", list_count);        // 总记录数
		maps.put("recordsFiltered", list_count);    // 过滤后的总记录数
		return maps;

	}

	/*@Override
	public ReturnT<String> syncConf(String appname, XxlConfUser loginUser, String loginEnv) {

		// valid
		XxlConfEnv xxlConfEnv = xxlConfEnvDao.load(loginEnv);
		if (xxlConfEnv == null) {
			return new ReturnT<String>(500, "配置Env非法");
		}
		XxlConfProject group = xxlConfProjectDao.load(appname);
		if (group==null) {
			return new ReturnT<String>(500, "AppName非法");
		}

		// project permission
		if (!ifHasProjectPermission(loginUser, loginEnv, appname)) {
			return new ReturnT<String>(500, "您没有该项目的配置权限,请联系管理员开通");
		}

		List<XxlConfNode> confNodeList = xxlConfNodeDao.pageList(0, 10000, loginEnv, appname, null);
		if (CollectionUtils.isEmpty(confNodeList)) {
			return new ReturnT<String>(500, "操作失败，该项目下不存在配置项");
		}

		// un sync node
		List<XxlConfNode> unSyncConfNodeList = new ArrayList<>();
		for (XxlConfNode node: confNodeList) {
			String realNodeValue = xxlConfZKManager.get(node.getEnv(), node.getKey());
			if (!node.getValue().equals(realNodeValue)) {
				unSyncConfNodeList.add(node);
			}
		}

		if (CollectionUtils.isEmpty(unSyncConfNodeList)) {
			return new ReturnT<String>(500, "操作失败，该项目下不存未同步的配置项");
		}

		// do sync
		String logContent = "操作成功，共计同步 " + unSyncConfNodeList.size() + " 条配置：";
		for (XxlConfNode node: unSyncConfNodeList) {

			xxlConfZKManager.set(node.getEnv(), node.getKey(), node.getValue());

			// node log
			XxlConfNodeLog nodeLog = new XxlConfNodeLog();
			nodeLog.setEnv(node.getEnv());
			nodeLog.setKey(node.getKey());
			nodeLog.setTitle(node.getTitle() + "(全量同步)" );
			nodeLog.setValue(node.getValue());
			nodeLog.setOptuser(loginUser.getUsername());
			xxlConfNodeLogDao.add(nodeLog);
			xxlConfNodeLogDao.deleteTimeout(node.getEnv(), node.getKey(), 10);

			logContent += "<br>" + node.getKey();
		}
		logContent.substring(logContent.length() - 1);

		return new ReturnT<String>(ReturnT.SUCCESS.getCode(), logContent);
	}*/

	@Override
	public ReturnT<String> delete(String key, XxlConfUser loginUser, String loginEnv) {
		if (StringUtils.isBlank(key)) {
			return new ReturnT<String>(500, "参数缺失");
		}
		XxlConfNode existNode = xxlConfNodeDao.load(loginEnv, key);
		if (existNode == null) {
			return new ReturnT<String>(500, "参数非法");
		}

		// project permission
		if (!ifHasProjectPermission(loginUser, loginEnv, existNode.getAppname())) {
			return new ReturnT<String>(500, "您没有该项目的配置权限,请联系管理员开通");
		}

		//xxlConfZKManager.delete(loginEnv, key);
		xxlConfNodeDao.delete(loginEnv, key);
		// length表示保留几条最新的，这里为0表示全部删除
		xxlConfNodeLogDao.deleteTimeout(loginEnv, key, 0);

		// write conf msg
		// 将msg写入db
		sendConfMsg(loginEnv, key, null);

		return ReturnT.SUCCESS;
	}

	// conf broadcast msg
	private void sendConfMsg(String env, String key, String value) {

		XxlConfNodeMsg confNodeMsg = new XxlConfNodeMsg();
		confNodeMsg.setEnv(env);
		confNodeMsg.setKey(key);
		confNodeMsg.setValue(value);

		xxlConfNodeMsgDao.add(confNodeMsg);
	}


	// ---------------------- start stop ----------------------

	/**
	 * 1、将key，value，env，appname都写到db中
	 * 2、向logTable中写入一条日志
	 * 3、写入message中
	 */
	@Override
	public ReturnT<String> add(XxlConfNode xxlConfNode, XxlConfUser loginUser, String loginEnv) {

		// valid
		if (StringUtils.isBlank(xxlConfNode.getAppname())) {
			return new ReturnT<String>(500, "AppName不可为空");
		}

		// project permission
		if (!ifHasProjectPermission(loginUser, loginEnv, xxlConfNode.getAppname())) {
			return new ReturnT<String>(500, "您没有该项目的配置权限,请联系管理员开通");
		}

		// valid group
		XxlConfProject group = xxlConfProjectDao.load(xxlConfNode.getAppname());
		if (group == null) {
			return new ReturnT<String>(500, "AppName非法");
		}

		// valid env
		if (StringUtils.isBlank(xxlConfNode.getEnv())) {
			return new ReturnT<String>(500, "配置Env不可为空");
		}
		XxlConfEnv xxlConfEnv = xxlConfEnvDao.load(xxlConfNode.getEnv());
		if (xxlConfEnv == null) {
			return new ReturnT<String>(500, "配置Env非法");
		}

		// valid key
		if (StringUtils.isBlank(xxlConfNode.getKey())) {
			return new ReturnT<String>(500, "配置Key不可为空");
		}
		xxlConfNode.setKey(xxlConfNode.getKey().trim());

		XxlConfNode existNode = xxlConfNodeDao.load(xxlConfNode.getEnv(), xxlConfNode.getKey());
		if (existNode != null) {
			return new ReturnT<String>(500, "配置Key已存在，不可重复添加");
		}
		if (!xxlConfNode.getKey().startsWith(xxlConfNode.getAppname())) {
			return new ReturnT<String>(500, "配置Key格式非法");
		}

		// valid title
		if (StringUtils.isBlank(xxlConfNode.getTitle())) {
			return new ReturnT<String>(500, "配置描述不可为空");
		}

		// value force null to ""
		if (xxlConfNode.getValue() == null) {
			xxlConfNode.setValue("");
		}

		// add node
		//xxlConfZKManager.set(xxlConfNode.getEnv(), xxlConfNode.getKey(), xxlConfNode.getValue());
		// 将key，value，env，appname都写到db中
		xxlConfNodeDao.insert(xxlConfNode);

		// node log
		XxlConfNodeLog nodeLog = new XxlConfNodeLog();
		nodeLog.setEnv(xxlConfNode.getEnv());
		nodeLog.setKey(xxlConfNode.getKey());
		nodeLog.setTitle(xxlConfNode.getTitle() + "(配置新增)");
		nodeLog.setValue(xxlConfNode.getValue());
		nodeLog.setOptuser(loginUser.getUsername());
		xxlConfNodeLogDao.add(nodeLog);

		// conf msg
		sendConfMsg(xxlConfNode.getEnv(), xxlConfNode.getKey(), xxlConfNode.getValue());

		return ReturnT.SUCCESS;
	}

	/**
	 * 1、更新title和value 当=env and = key
	 * 2、向logTable中写入一条日志
	 * 3、从logTable中删除，只留10条日志
	 * 3、写入message中
	 */
	@Override
	public ReturnT<String> update(XxlConfNode xxlConfNode, XxlConfUser loginUser, String loginEnv) {

		// valid
		if (StringUtils.isBlank(xxlConfNode.getKey())) {
			return new ReturnT<String>(500, "配置Key不可为空");
		}
		XxlConfNode existNode = xxlConfNodeDao.load(xxlConfNode.getEnv(), xxlConfNode.getKey());
		if (existNode == null) {
			return new ReturnT<String>(500, "配置Key非法");
		}

		// project permission
		if (!ifHasProjectPermission(loginUser, loginEnv, existNode.getAppname())) {
			return new ReturnT<String>(500, "您没有该项目的配置权限,请联系管理员开通");
		}

		if (StringUtils.isBlank(xxlConfNode.getTitle())) {
			return new ReturnT<String>(500, "配置描述不可为空");
		}

		// value force null to ""
		if (xxlConfNode.getValue() == null) {
			xxlConfNode.setValue("");
		}

		// update conf
		//xxlConfZKManager.set(xxlConfNode.getEnv(), xxlConfNode.getKey(), xxlConfNode.getValue());

		existNode.setTitle(xxlConfNode.getTitle());
		existNode.setValue(xxlConfNode.getValue());
		int ret = xxlConfNodeDao.update(existNode);
		if (ret < 1) {
			return ReturnT.FAIL;
		}

		// node log
		XxlConfNodeLog nodeLog = new XxlConfNodeLog();
		nodeLog.setEnv(existNode.getEnv());
		nodeLog.setKey(existNode.getKey());
		nodeLog.setTitle(existNode.getTitle() + "(配置更新)");
		nodeLog.setValue(existNode.getValue());
		nodeLog.setOptuser(loginUser.getUsername());
		xxlConfNodeLogDao.add(nodeLog);
		xxlConfNodeLogDao.deleteTimeout(existNode.getEnv(), existNode.getKey(), 10);

		// conf msg
		sendConfMsg(xxlConfNode.getEnv(), xxlConfNode.getKey(), xxlConfNode.getValue());

		return ReturnT.SUCCESS;
	}


	// ---------------------- thread ----------------------

	/**
	 * 从文件中读取数据
	 */
	@Override
	public ReturnT<Map<String, String>> find(String accessToken, String env, List<String> keys) {

		// valid
		if (this.accessToken != null && this.accessToken.trim().length() > 0 && !this.accessToken.equals(accessToken)) {
			return new ReturnT<Map<String, String>>(ReturnT.FAIL.getCode(), "AccessToken Invalid.");
		}
		if (env == null || env.trim().length() == 0) {
			return new ReturnT<>(ReturnT.FAIL.getCode(), "env Invalid.");
		}
		if (keys == null || keys.size() == 0) {
			return new ReturnT<>(ReturnT.FAIL.getCode(), "keys Invalid.");
		}
		/*for (String key: keys) {
			if (key==null || key.trim().length()<4 || key.trim().length()>100) {
				return new ReturnT<>(ReturnT.FAIL.getCode(), "Key Invalid[4~100]");
			}
			if (!RegexUtil.matches(RegexUtil.abc_number_line_point_pattern, key)) {
				return new ReturnT<>(ReturnT.FAIL.getCode(), "Key format Invalid");
			}
		}*/

		// result
		Map<String, String> result = new HashMap<String, String>();
		for (String key : keys) {

			// get val
			String value = null;
			if (key == null || key.trim().length() < 4 || key.trim().length() > 100
					|| !RegexUtil.matches(RegexUtil.abc_number_line_point_pattern, key)) {
				// invalid key, pass
			} else {
				value = getFileConfData(env, key);
			}

			// parse null
			if (value == null) {
				value = "";
			}

			// put
			result.put(key, value);
		}

		return new ReturnT<Map<String, String>>(result);
	}

	@Override
	public DeferredResult<ReturnT<String>> monitor(String accessToken, String env, List<String> keys) {

		// 初始化一个result，超时结果为success，
		// DeferredResult的使用方法为 调用setResult()方法时立即向浏览器发出响应；未调用时请求被挂起
		DeferredResult deferredResult = new DeferredResult(confBeatTime * 1000L, new ReturnT<>(ReturnT.SUCCESS_CODE, "Monitor timeout, no key updated."));

		// valid
		if (this.accessToken != null && this.accessToken.trim().length() > 0 && !this.accessToken.equals(accessToken)) {
			deferredResult.setResult(new ReturnT<>(ReturnT.FAIL.getCode(), "AccessToken Invalid."));
			return deferredResult;
		}
		if (env == null || env.trim().length() == 0) {
			deferredResult.setResult(new ReturnT<>(ReturnT.FAIL.getCode(), "env Invalid."));
			return deferredResult;
		}
		if (keys == null || keys.size() == 0) {
			deferredResult.setResult(new ReturnT<>(ReturnT.FAIL.getCode(), "keys Invalid."));
			return deferredResult;
		}
		/*for (String key: keys) {
			if (key==null || key.trim().length()<4 || key.trim().length()>100) {
				deferredResult.setResult(new ReturnT<>(ReturnT.FAIL.getCode(), "Key Invalid[4~100]"));
				return deferredResult;
			}
			if (!RegexUtil.matches(RegexUtil.abc_number_line_point_pattern, key)) {
				deferredResult.setResult(new ReturnT<>(ReturnT.FAIL.getCode(), "Key format Invalid"));
				return deferredResult;
			}
		}*/

		// monitor by client
		for (String key : keys) {
			// invalid key, pass
			if (key == null || key.trim().length() < 4 || key.trim().length() > 100
					|| !RegexUtil.matches(RegexUtil.abc_number_line_point_pattern, key)) {
				continue;
			}

			// monitor each key
			// 获取env+key对应的fileName
			String fileName = parseConfDataFileName(env, key);

			// Map<fileName, List<DeferredResult>>
			List<DeferredResult> deferredResultList = confDeferredResultMap.get(fileName);
			if (deferredResultList == null) {
				deferredResultList = new ArrayList<>();
				confDeferredResultMap.put(fileName, deferredResultList);
			}

			deferredResultList.add(deferredResult);
		}

		return deferredResult;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		startThead();
	}

	@Override
	public void destroy() throws Exception {
		stopThread();
	}

	/**
	 * 1、同步了所有改变过的key对应的value到文件中（被改变过的key存储在msg表中）
	 * 2、同步了所有的key对应的value到文件中（key存储在conf表中）
	 */
	public void startThead() throws Exception {

		/**
		 * broadcast conf-data msg, sync to file, for "add、update、delete"
		 * 1、将db中的msg写入文件
		 * 2、将confDeferredResultMap中的fileName进行删除，并且将value中的DeferredResult都设置为success
		 * 3、清除所有不在conf表中的本地文件
		 */
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				while (!executorStoped) {
					try {
						// new message, filter readed
						List<XxlConfNodeMsg> messageList = xxlConfNodeMsgDao.findMsgExclude(readedMessageIds);
						if (messageList != null && messageList.size() > 0) {
							for (XxlConfNodeMsg message : messageList) {
								readedMessageIds.add(message.getId());

								// sync file
								setFileConfData(message.getEnv(), message.getKey(), message.getValue());
							}
						}

						// clean old message;
						// 这个我理解干了两件事，保留30秒内的数据，清除readedMessageIds
						if ((System.currentTimeMillis() / 1000) % confBeatTime == 0) {
							xxlConfNodeMsgDao.cleanMessage(confBeatTime);
							readedMessageIds.clear();
						}
					} catch (Exception e) {
						if (!executorStoped) {
							logger.error(e.getMessage(), e);
						}
					}
					try {
						TimeUnit.SECONDS.sleep(1);
					} catch (Exception e) {
						if (!executorStoped) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			}
		});


		/**
		 *  sync total conf-data, db + file      (1+N/30s)
		 *
		 *  clean deleted conf-data file
		 */
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				while (!executorStoped) {

					// align to beattime
					// 对齐时间
					try {
						long sleepSecond = confBeatTime - (System.currentTimeMillis() / 1000) % confBeatTime;
						if (sleepSecond > 0 && sleepSecond < confBeatTime) {
							TimeUnit.SECONDS.sleep(sleepSecond);
						}
					} catch (Exception e) {
						if (!executorStoped) {
							logger.error(e.getMessage(), e);
						}
					}

					try {

						// sync registry-data, db + file
						int offset = 0;
						int pagesize = 1000;

						// 这里存储了所有conf的配置文件
						List<String> confDataFileList = new ArrayList<>();

						// 1、遍历所有conf配置，每次读1000条数据
						List<XxlConfNode> confNodeList = xxlConfNodeDao.pageList(offset, pagesize, null, null, null);
						while (confNodeList != null && confNodeList.size() > 0) {

							for (XxlConfNode confNoteItem : confNodeList) {

								// sync file
								String confDataFile = setFileConfData(confNoteItem.getEnv(), confNoteItem.getKey(), confNoteItem.getValue());

								// collect confDataFile
								confDataFileList.add(confDataFile);
							}


							offset += 1000;
							confNodeList = xxlConfNodeDao.pageList(offset, pagesize, null, null, null);
						}

						// clean old registry-data file
						// 清除所有不在conf表中的本地文件
						cleanFileConfDataExclude(confDataFileList);

						logger.debug(">>>>>>>>>>> xxl-conf, sync totel conf data success, sync conf count = {}", confDataFileList.size());
					} catch (Exception e) {
						if (!executorStoped) {
							logger.error(e.getMessage(), e);
						}
					}
					try {
						TimeUnit.SECONDS.sleep(confBeatTime);
					} catch (Exception e) {
						if (!executorStoped) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			}
		});


	}

	private void stopThread() {
		executorStoped = true;
		executorService.shutdownNow();
	}


	// ---------------------- file opt ----------------------

	/**
	 * get
	 * 1、读取env+key对应的文件
	 * 2、从文件中获取value
	 */
	public String getFileConfData(String env, String key) {

		// fileName
		String confFileName = parseConfDataFileName(env, key);

		// read
		Properties existProp = PropUtil.loadFileProp(confFileName);
		if (existProp != null && existProp.containsKey("value")) {
			return existProp.getProperty("value");
		}
		return null;
	}

	private String parseConfDataFileName(String env, String key) {
		// fileName
		String fileName = confDataFilePath
				.concat(File.separator).concat(env)
				.concat(File.separator).concat(key)
				.concat(".properties");
		return fileName;
	}

	/**
	 * set
	 * 1、文件存在且相等，直接返回文件地址
	 * 2、写入文件
	 * 3、将confDeferredResultMap中的fileName进行删除，并且将value中的DeferredResult都设置为success
	 */

	private String setFileConfData(String env, String key, String value) {

		// fileName
		String confFileName = parseConfDataFileName(env, key);

		// valid repeat update
		// 1、文件存在且相等，直接返回文件地址
		Properties existProp = PropUtil.loadFileProp(confFileName);
		if (existProp != null
				&& value != null
				&& value.equals(existProp.getProperty("value"))
		) {
			return new File(confFileName).getPath();
		}

		// write
		// 2、写入文件
		Properties prop = new Properties();
		if (value == null) {
			prop.setProperty("value-deleted", "true");
		} else {
			prop.setProperty("value", value);
		}

		PropUtil.writeFileProp(prop, confFileName);
		logger.info(">>>>>>>>>>> xxl-conf, setFileConfData: confFileName={}, value={}", confFileName, value);

		// broadcast monitor client
		// 3、将confDeferredResultMap中的fileName进行删除，并且将value中的DeferredResult都设置为success，这时候阻塞的monitor会返回
		List<DeferredResult> deferredResultList = confDeferredResultMap.get(confFileName);
		if (deferredResultList != null) {
			confDeferredResultMap.remove(confFileName);
			for (DeferredResult deferredResult : deferredResultList) {
				deferredResult.setResult(new ReturnT<>(ReturnT.SUCCESS_CODE, "Monitor key update."));
			}
		}

		return new File(confFileName).getPath();
	}

	// clean
	public void cleanFileConfDataExclude(List<String> confDataFileList) {
		filterChildPathExclude(new File(confDataFilePath), confDataFileList);
	}

	// 删除所有不在confDataFileList中的文件
	public void filterChildPathExclude(File parentPath, final List<String> confDataFileList) {
		if (!parentPath.exists() || parentPath.list() == null || parentPath.list().length == 0) {
			return;
		}
		File[] childFileList = parentPath.listFiles();
		for (File childFile : childFileList) {
			if (childFile.isFile() && !confDataFileList.contains(childFile.getPath())) {
				childFile.delete();

				logger.info(">>>>>>>>>>> xxl-conf, cleanFileConfData, ConfDataFile={}", childFile.getPath());
			}
			if (childFile.isDirectory()) {
				if (parentPath.listFiles() != null && parentPath.listFiles().length > 0) {
					filterChildPathExclude(childFile, confDataFileList);
				} else {
					childFile.delete();
				}

			}
		}

	}

}
