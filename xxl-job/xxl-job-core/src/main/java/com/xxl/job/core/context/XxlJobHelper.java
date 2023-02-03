package com.xxl.job.core.context;

import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

/**
 * helper for xxl-job
 *
 * @author xuxueli 2020-11-05
 */
public class XxlJobHelper {

	// ---------------------- base info ----------------------

	private static Logger logger = LoggerFactory.getLogger("xxl-job logger");

	/**
	 * current JobId
	 *
	 * @return
	 */
	public static long getJobId() {
		XxlJobContext xxlJobContext = XxlJobContext.getXxlJobContext();
		if (xxlJobContext == null) {
			return -1;
		}

		return xxlJobContext.getJobId();
	}

	// ---------------------- for log ----------------------

	/**
	 * current JobParam
	 *
	 * @return
	 */
	public static String getJobParam() {
		XxlJobContext xxlJobContext = XxlJobContext.getXxlJobContext();
		if (xxlJobContext == null) {
			return null;
		}

		return xxlJobContext.getJobParam();
	}

	// ---------------------- for shard ----------------------

	/**
	 * current JobLogFileName
	 *
	 * @return
	 */
	public static String getJobLogFileName() {
		XxlJobContext xxlJobContext = XxlJobContext.getXxlJobContext();
		if (xxlJobContext == null) {
			return null;
		}

		return xxlJobContext.getJobLogFileName();
	}

	/**
	 * current ShardIndex
	 *
	 * @return
	 */
	public static int getShardIndex() {
		XxlJobContext xxlJobContext = XxlJobContext.getXxlJobContext();
		if (xxlJobContext == null) {
			return -1;
		}

		return xxlJobContext.getShardIndex();
	}

	// ---------------------- tool for log ----------------------

	/**
	 * current ShardTotal
	 *
	 * @return
	 */
	public static int getShardTotal() {
		XxlJobContext xxlJobContext = XxlJobContext.getXxlJobContext();
		if (xxlJobContext == null) {
			return -1;
		}

		return xxlJobContext.getShardTotal();
	}

	/**
	 * append log with pattern
	 *
	 * @param appendLogPattern   like "aaa {} bbb {} ccc"
	 * @param appendLogArguments like "111, true"
	 */
	public static boolean log(String appendLogPattern, Object... appendLogArguments) {

		// 使用slf4j解析器格式化日志内容
		FormattingTuple ft = MessageFormatter.arrayFormat(appendLogPattern, appendLogArguments);
		String appendLog = ft.getMessage();

        /*appendLog = appendLogPattern;
        if (appendLogArguments!=null && appendLogArguments.length>0) {
            appendLog = MessageFormat.format(appendLogPattern, appendLogArguments);
        }*/

		// 这是获得调用栈帧方法，索引0为当前栈帧，1为调用栈帧，以此类推，此处获得的是索引1，也就是说获得的是调用该方法的栈帧信息，
		// 可以通过StackTraceElement获得调用类名，方法名，行数等信息
		StackTraceElement callInfo = new Throwable().getStackTrace()[1];
		return logDetail(callInfo, appendLog);
	}

	/**
	 * append exception stack
	 *
	 * @param e
	 */
	public static boolean log(Throwable e) {

		StringWriter stringWriter = new StringWriter();
		e.printStackTrace(new PrintWriter(stringWriter));
		String appendLog = stringWriter.toString();

		StackTraceElement callInfo = new Throwable().getStackTrace()[1];
		return logDetail(callInfo, appendLog);
	}

	/**
	 * append log
	 * xxl-job将日志都写入了本地文件并没有推送给服务端，此处日志并非使用的是推的模式，而是使用的拉模式，当我们后台打开任务日志时，服务端会到客户端来拉取日志
	 */
	private static boolean logDetail(StackTraceElement callInfo, String appendLog) {
		// 获得当前上下文对象
		XxlJobContext xxlJobContext = XxlJobContext.getXxlJobContext();
		if (xxlJobContext == null) {
			return false;
		}

        /*// "yyyy-MM-dd HH:mm:ss [ClassName]-[MethodName]-[LineNumber]-[ThreadName] log";
        StackTraceElement[] stackTraceElements = new Throwable().getStackTrace();
        StackTraceElement callInfo = stackTraceElements[1];*/

		// 拼接格式化日志信息
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append(DateUtil.formatDateTime(new Date())).append(" ")
				.append("[" + callInfo.getClassName() + "#" + callInfo.getMethodName() + "]").append("-")
				.append("[" + callInfo.getLineNumber() + "]").append("-")
				.append("[" + Thread.currentThread().getName() + "]").append(" ")
				.append(appendLog != null ? appendLog : "");
		String formatAppendLog = stringBuffer.toString();

		// appendlog
		// 获得日志文件路径
		String logFileName = xxlJobContext.getJobLogFileName();

		if (logFileName != null && logFileName.trim().length() > 0) {
			// 流的形式将日志写入本地文件
			XxlJobFileAppender.appendLog(logFileName, formatAppendLog);
			return true;
		} else {
			logger.info(">>>>>>>>>>> {}", formatAppendLog);
			return false;
		}
	}

	// ---------------------- tool for handleResult ----------------------

	/**
	 * handle success
	 *
	 * @return
	 */
	public static boolean handleSuccess() {
		return handleResult(XxlJobContext.HANDLE_CODE_SUCCESS, null);
	}

	/**
	 * handle success with log msg
	 *
	 * @param handleMsg
	 * @return
	 */
	public static boolean handleSuccess(String handleMsg) {
		return handleResult(XxlJobContext.HANDLE_CODE_SUCCESS, handleMsg);
	}

	/**
	 * handle fail
	 *
	 * @return
	 */
	public static boolean handleFail() {
		return handleResult(XxlJobContext.HANDLE_CODE_FAIL, null);
	}

	/**
	 * handle fail with log msg
	 *
	 * @param handleMsg
	 * @return
	 */
	public static boolean handleFail(String handleMsg) {
		return handleResult(XxlJobContext.HANDLE_CODE_FAIL, handleMsg);
	}

	/**
	 * handle timeout
	 *
	 * @return
	 */
	public static boolean handleTimeout() {
		return handleResult(XxlJobContext.HANDLE_CODE_TIMEOUT, null);
	}

	/**
	 * handle timeout with log msg
	 *
	 * @param handleMsg
	 * @return
	 */
	public static boolean handleTimeout(String handleMsg) {
		return handleResult(XxlJobContext.HANDLE_CODE_TIMEOUT, handleMsg);
	}

	/**
	 * @param handleCode 200 : success
	 *                   500 : fail
	 *                   502 : timeout
	 * @param handleMsg
	 * @return
	 */
	public static boolean handleResult(int handleCode, String handleMsg) {
		XxlJobContext xxlJobContext = XxlJobContext.getXxlJobContext();
		if (xxlJobContext == null) {
			return false;
		}

		xxlJobContext.setHandleCode(handleCode);
		if (handleMsg != null) {
			xxlJobContext.setHandleMsg(handleMsg);
		}
		return true;
	}


}
