/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.action.ssh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.util.StringUtils;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

/**
 * Ssh action executor. <p/> <ul> <li>Execute the shell commands on the remote host</li> <li>Copies the base and wrapper
 * scripts on to the remote location</li> <li>Base script is used to run the command on the remote host</li> <li>Wrapper
 * script is used to check the status of the submitted command</li> <li>handles the submission failures</li> </ul>
 */
public class SshActionExecutor extends ActionExecutor {
    public static final String ACTION_TYPE = "ssh";

    /**
     * Configuration parameter which specifies whether the specified ssh user is allowed, or has to be the job user.
     */
    public static final String CONF_SSH_ALLOW_USER_AT_HOST = CONF_PREFIX + "ssh.allow.user.at.host";

    protected static final String SSH_COMMAND_OPTIONS =
            "-o PasswordAuthentication=no -o KbdInteractiveDevices=no -o StrictHostKeyChecking=no -o ConnectTimeout=20 ";

    public static final String COMMAND_BASE_PORT = "oozie.action.ssh.command.port";

    protected static String SSH_COMMAND_BASE = "ssh -p ";
    protected static String SCP_COMMAND_BASE = "scp -P ";

    public static final String ERR_SETUP_FAILED = "SETUP_FAILED";
    public static final String ERR_EXECUTION_FAILED = "EXECUTION_FAILED";
    public static final String ERR_UNKNOWN_ERROR = "UNKOWN_ERROR";
    public static final String ERR_COULD_NOT_CONNECT = "COULD_NOT_CONNECT";
    public static final String ERR_HOST_RESOLUTION = "COULD_NOT_RESOLVE_HOST";
    public static final String ERR_FNF = "FNF";
    public static final String ERR_AUTH_FAILED = "AUTH_FAILED";
    public static final String ERR_NO_EXEC_PERM = "NO_EXEC_PERM";
    public static final String ERR_USER_MISMATCH = "ERR_USER_MISMATCH";
    public static final String ERR_EXCEDE_LEN = "ERR_OUTPUT_EXCEED_MAX_LEN";

    public static final String DELETE_TMP_DIR = "oozie.action.ssh.delete.remote.tmp.dir";

    public static final String HTTP_COMMAND = "oozie.action.ssh.http.command";

    public static final String HTTP_COMMAND_OPTIONS = "oozie.action.ssh.http.command.post.options";

    private static final String EXT_STATUS_VAR = "#status";

    private static int maxLen;
    private static boolean allowSshUserAtHost;

    protected SshActionExecutor() {
        super(ACTION_TYPE);
    }

    /**
     * Initialize Action.
     */
    @Override
    public void initActionType() {
        super.initActionType();
        maxLen = getOozieConf().getInt(CallbackServlet.CONF_MAX_DATA_LEN, 2 * 1024);
        allowSshUserAtHost = getOozieConf().getBoolean(CONF_SSH_ALLOW_USER_AT_HOST, true);

        String portId = getOozieConf().get(COMMAND_BASE_PORT, "22");
        SSH_COMMAND_BASE += portId + " " +  SSH_COMMAND_OPTIONS;
        SCP_COMMAND_BASE += portId + " " +  SSH_COMMAND_OPTIONS;

        registerError(InterruptedException.class.getName(), ActionExecutorException.ErrorType.ERROR, "SH001");
        registerError(JDOMException.class.getName(), ActionExecutorException.ErrorType.ERROR, "SH002");
        initSshScripts();
    }

    /**
     * Check ssh action status.
     *
     * @param context action execution context.
     * @param action action object.
     */
    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        Status status = getActionStatus(context, action);
        boolean captureOutput = false;
        try {
            Element eConf = XmlUtils.parseXml(action.getConf());
            Namespace ns = eConf.getNamespace();
            captureOutput = eConf.getChild("capture-output", ns) != null;
        }
        catch (JDOMException ex) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "ERR_XML_PARSE_FAILED",
                                              "unknown error", ex);
        }
        XLog log = XLog.getLog(getClass());
        log.debug("Capture Output: {0}", captureOutput);
        if (status == Status.OK) {
            if (captureOutput) {
                String outFile = getRemoteFileName(context, action, "stdout", false, true);
                String dataCommand = SSH_COMMAND_BASE + action.getTrackerUri() + " cat " + outFile;
                log.debug("Ssh command [{0}]", dataCommand);
                try {
                    Process process = Runtime.getRuntime().exec(dataCommand.split("\\s"));
                    StringBuffer buffer = new StringBuffer();
                    boolean overflow = false;
                    drainBuffers(process, buffer, null, maxLen);
                    if (buffer.length() > maxLen) {
                        overflow = true;
                    }
                    if (overflow) {
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR,
                                                          "ERR_OUTPUT_EXCEED_MAX_LEN", "unknown error");
                    }
                    context.setExecutionData(status.toString(), PropertiesUtils.stringToProperties(buffer.toString()));
                }
                catch (Exception ex) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "ERR_UNKNOWN_ERROR",
                                                      "unknown error", ex);
                }
            }
            else {
                context.setExecutionData(status.toString(), null);
            }
        }
        else {
            if (status == Status.ERROR) {
                context.setExecutionData(status.toString(), null);
            }
            else {
                context.setExternalStatus(status.toString());
            }
        }
    }

    /**
     * Kill ssh action.
     *
     * @param context action execution context.
     * @param action object.
     */
    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        String command = "ssh " + action.getTrackerUri() + " kill  -KILL " + action.getExternalId();
        int returnValue = getReturnValue(command);
        if (returnValue != 0) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FAILED_TO_KILL", XLog.format(
                    "Unable to kill process {0} on {1}", action.getExternalId(), action.getTrackerUri()));
        }
        context.setEndData(WorkflowAction.Status.KILLED, "ERROR");
    }

    /**
     * Start the ssh action execution.
     *
     * @param context action execution context.
     * @param action action object.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void start(final Context context, final WorkflowAction action) throws ActionExecutorException {
        XLog log = XLog.getLog(getClass());
        log.info("start() begins");
        String confStr = action.getConf();
        Element conf;
        try {
            conf = XmlUtils.parseXml(confStr);
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
        Namespace nameSpace = conf.getNamespace();
        Element hostElement = conf.getChild("host", nameSpace);
        String hostString = hostElement.getValue().trim();
        hostString = prepareUserHost(hostString, context);
        final String host = hostString;
        final String dirLocation = execute(new Callable<String>() {
            public String call() throws Exception {
                return setupRemote(host, context, action);
            }

        });

        String runningPid = execute(new Callable<String>() {
            public String call() throws Exception {
                return checkIfRunning(host, context, action);
            }
        });
        String pid = "";

        if (runningPid == null) {
            final Element commandElement = conf.getChild("command", nameSpace);
            final boolean ignoreOutput = conf.getChild("capture-output", nameSpace) == null;

            boolean preserve = false;
            if (commandElement != null) {
                String[] args = null;
                // Will either have <args>, <arg>, or neither (but not both)
                List<Element> argsList = conf.getChildren("args", nameSpace);
                // Arguments in an <args> are "flattened" (spaces are delimiters)
                if (argsList != null && argsList.size() > 0) {
                    StringBuilder argsString = new StringBuilder("");
                    for (Element argsElement : argsList) {
                        argsString = argsString.append(argsElement.getValue()).append(" ");
                    }
                    args = new String[]{argsString.toString()};
                }
                else {
                    // Arguments in an <arg> are preserved, even with spaces
                    argsList = conf.getChildren("arg", nameSpace);
                    if (argsList != null && argsList.size() > 0) {
                        preserve = true;
                        args = new String[argsList.size()];
                        for (int i = 0; i < argsList.size(); i++) {
                            Element argsElement = argsList.get(i);
                            args[i] = argsElement.getValue();
                            // Even though we're keeping the args as an array, if they contain a space we still have to either quote
                            // them or escape their space (because the scripts will split them up otherwise)
                            if (args[i].contains(" ") &&
                                    !(args[i].startsWith("\"") && args[i].endsWith("\"") ||
                                      args[i].startsWith("'") && args[i].endsWith("'"))) {
                                args[i] = StringUtils.escapeString(args[i], '\\', ' ');
                            }
                        }
                    }
                }
                final String[] argsF = args;
                final String recoveryId = context.getRecoveryId();
                final boolean preserveF = preserve;
                pid = execute(new Callable<String>() {

                    @Override
                    public String call() throws Exception {
                        return doExecute(host, dirLocation, commandElement.getValue(), argsF, ignoreOutput, action, recoveryId,
                                preserveF);
                    }

                });
            }
            context.setStartData(pid, host, host);
        }
        else {
            pid = runningPid;
            context.setStartData(pid, host, host);
            check(context, action);
        }
        log.info("start() ends");
    }

    private String checkIfRunning(String host, final Context context, final WorkflowAction action) {
        String pid = null;
        String outFile = getRemoteFileName(context, action, "pid", false, false);
        String getOutputCmd = SSH_COMMAND_BASE + host + " cat " + outFile;
        try {
            Process process = Runtime.getRuntime().exec(getOutputCmd.split("\\s"));
            StringBuffer buffer = new StringBuffer();
            drainBuffers(process, buffer, null, maxLen);
            pid = getFirstLine(buffer);

            if (Long.valueOf(pid) > 0) {
                return pid;
            }
            else {
                return null;
            }
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Get remote host working location.
     *
     * @param context action execution context
     * @param action Action
     * @param fileExtension Extension to be added to file name
     * @param dirOnly Get the Directory only
     * @param useExtId Flag to use external ID in the path
     * @return remote host file name/Directory.
     */
    public String getRemoteFileName(Context context, WorkflowAction action, String fileExtension, boolean dirOnly,
                                    boolean useExtId) {
        String path = getActionDirPath(context.getWorkflow().getId(), action, ACTION_TYPE, false) + "/";
        if (dirOnly) {
            return path;
        }
        if (useExtId) {
            path = path + action.getExternalId() + ".";
        }
        path = path + context.getRecoveryId() + "." + fileExtension;
        return path;
    }

    /**
     * Utility method to execute command.
     *
     * @param command Command to execute as String.
     * @return exit status of the execution.
     * @throws IOException if process exits with status nonzero.
     * @throws InterruptedException if process does not run properly.
     */
    public int executeCommand(String command) throws IOException, InterruptedException {
        Runtime runtime = Runtime.getRuntime();
        Process p = runtime.exec(command.split("\\s"));

        StringBuffer errorBuffer = new StringBuffer();
        int exitValue = drainBuffers(p, null, errorBuffer, maxLen);

        String error = null;
        if (exitValue != 0) {
            error = getTruncatedString(errorBuffer);
            throw new IOException(XLog.format("Not able to perform operation [{0}]", command) + " | " + "ErrorStream: "
                    + error);
        }
        return exitValue;
    }

    /**
     * Do ssh action execution setup on remote host.
     *
     * @param host host name.
     * @param context action execution context.
     * @param action action object.
     * @return remote host working directory.
     * @throws IOException thrown if failed to setup.
     * @throws InterruptedException thrown if any interruption happens.
     */
    protected String setupRemote(String host, Context context, WorkflowAction action) throws IOException, InterruptedException {
        XLog log = XLog.getLog(getClass());
        log.info("Attempting to copy ssh base scripts to remote host [{0}]", host);
        String localDirLocation = Services.get().getRuntimeDir() + "/ssh";
        if (localDirLocation.endsWith("/")) {
            localDirLocation = localDirLocation.substring(0, localDirLocation.length() - 1);
        }
        File file = new File(localDirLocation + "/ssh-base.sh");
        if (!file.exists()) {
            throw new IOException("Required Local file " + file.getAbsolutePath() + " not present.");
        }
        file = new File(localDirLocation + "/ssh-wrapper.sh");
        if (!file.exists()) {
            throw new IOException("Required Local file " + file.getAbsolutePath() + " not present.");
        }
        String remoteDirLocation = getRemoteFileName(context, action, null, true, true);
        String command = XLog.format("{0}{1}  mkdir -p {2} ", SSH_COMMAND_BASE, host, remoteDirLocation).toString();
        executeCommand(command);
        command = XLog.format("{0}{1}/ssh-base.sh {2}/ssh-wrapper.sh {3}:{4}", SCP_COMMAND_BASE, localDirLocation,
                              localDirLocation, host, remoteDirLocation);
        executeCommand(command);
        command = XLog.format("{0}{1}  chmod +x {2}ssh-base.sh {3}ssh-wrapper.sh ", SSH_COMMAND_BASE, host,
                              remoteDirLocation, remoteDirLocation);
        executeCommand(command);
        return remoteDirLocation;
    }

    /**
     * Execute the ssh command.
     *
     * @param host hostname.
     * @param dirLocation location of the base and wrapper scripts.
     * @param cmnd command to be executed.
     * @param args command arguments.
     * @param ignoreOutput ignore output option.
     * @param action action object.
     * @param recoveryId action id + run number to enable recovery in rerun
     * @param preserveArgs tell the ssh scripts to preserve or flatten the arguments
     * @return process id of the running command.
     * @throws IOException thrown if failed to run the command.
     * @throws InterruptedException thrown if any interruption happens.
     */
    protected String doExecute(String host, String dirLocation, String cmnd, String[] args, boolean ignoreOutput,
                               WorkflowAction action, String recoveryId, boolean preserveArgs)
                               throws IOException, InterruptedException {
        XLog log = XLog.getLog(getClass());
        Runtime runtime = Runtime.getRuntime();
        String callbackPost = ignoreOutput ? "_" : getOozieConf().get(HTTP_COMMAND_OPTIONS).replace(" ", "%%%");
        String preserveArgsS = preserveArgs ? "PRESERVE_ARGS" : "FLATTEN_ARGS";
        // TODO check
        String callBackUrl = Services.get().get(CallbackService.class)
                .createCallBackUrl(action.getId(), EXT_STATUS_VAR);
        String command = XLog.format("{0}{1} {2}ssh-base.sh {3} {4} \"{5}\" \"{6}\" {7} {8} ", SSH_COMMAND_BASE, host, dirLocation,
                preserveArgsS, getOozieConf().get(HTTP_COMMAND), callBackUrl, callbackPost, recoveryId, cmnd)
                .toString();
        String[] commandArray = command.split("\\s");
        String[] finalCommand;
        if (args == null) {
            finalCommand = commandArray;
        }
        else {
            finalCommand = new String[commandArray.length + args.length];
            System.arraycopy(commandArray, 0, finalCommand, 0, commandArray.length);
            System.arraycopy(args, 0, finalCommand, commandArray.length, args.length);
        }
        log.trace("Executing ssh command [{0}]", Arrays.toString(finalCommand));
        Process p = runtime.exec(finalCommand);
        String pid = "";

        StringBuffer inputBuffer = new StringBuffer();
        StringBuffer errorBuffer = new StringBuffer();
        int exitValue = drainBuffers(p, inputBuffer, errorBuffer, maxLen);

        pid = getFirstLine(inputBuffer);

        String error = null;
        if (exitValue != 0) {
            error = getTruncatedString(errorBuffer);
            throw new IOException(XLog.format("Not able to execute ssh-base.sh on {0}", host) + " | " + "ErrorStream: "
                    + error);
        }
        return pid;
    }

    /**
     * End action execution.
     *
     * @param context action execution context.
     * @param action action object.
     * @throws ActionExecutorException thrown if action end execution fails.
     */
    public void end(final Context context, final WorkflowAction action) throws ActionExecutorException {
        if (action.getExternalStatus().equals("OK")) {
            context.setEndData(WorkflowAction.Status.OK, WorkflowAction.Status.OK.toString());
        }
        else {
            context.setEndData(WorkflowAction.Status.ERROR, WorkflowAction.Status.ERROR.toString());
        }
        boolean deleteTmpDir = getOozieConf().getBoolean(DELETE_TMP_DIR, true);
        if (deleteTmpDir) {
            String tmpDir = getRemoteFileName(context, action, null, true, false);
            String removeTmpDirCmd = SSH_COMMAND_BASE + action.getTrackerUri() + " rm -rf " + tmpDir;
            int retVal = getReturnValue(removeTmpDirCmd);
            if (retVal != 0) {
                XLog.getLog(getClass()).warn("Cannot delete temp dir {0}", tmpDir);
            }
        }
    }

    /**
     * Get the return value of a process.
     *
     * @param command command to be executed.
     * @return zero if execution is successful and any non zero value for failure.
     * @throws ActionExecutorException
     */
    private int getReturnValue(String command) throws ActionExecutorException {
        int returnValue;
        Process ps = null;
        try {
            ps = Runtime.getRuntime().exec(command.split("\\s"));
            returnValue = drainBuffers(ps, null, null, 0);
        }
        catch (IOException e) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FAILED_OPERATION", XLog.format(
                    "Not able to perform operation {0}", command), e);
        }
        finally {
            ps.destroy();
        }
        return returnValue;
    }

    /**
     * Copy the ssh base and wrapper scripts to the local directory.
     */
    private void initSshScripts() {
        String dirLocation = Services.get().getRuntimeDir() + "/ssh";
        File path = new File(dirLocation);
        path.mkdirs();
        if (!path.exists()) {
            throw new RuntimeException(XLog.format("Not able to create required directory {0}", dirLocation));
        }
        try {
            IOUtils.copyCharStream(IOUtils.getResourceAsReader("ssh-base.sh", -1), new FileWriter(dirLocation
                    + "/ssh-base.sh"));
            IOUtils.copyCharStream(IOUtils.getResourceAsReader("ssh-wrapper.sh", -1), new FileWriter(dirLocation
                    + "/ssh-wrapper.sh"));
        }
        catch (IOException ie) {
            throw new RuntimeException(XLog.format("Not able to copy required scripts file to {0} "
                    + "for SshActionHandler", dirLocation));
        }
    }

    /**
     * Get action status.
     *
     * @param action action object.
     * @return status of the action(RUNNING/OK/ERROR).
     * @throws ActionExecutorException thrown if there is any error in getting status.
     */
    protected Status getActionStatus(Context context, WorkflowAction action) throws ActionExecutorException {
        String command = SSH_COMMAND_BASE + action.getTrackerUri() + " ps -p " + action.getExternalId();
        Status aStatus;
        int returnValue = getReturnValue(command);
        if (returnValue == 0) {
            aStatus = Status.RUNNING;
        }
        else {
            String outFile = getRemoteFileName(context, action, "error", false, true);
            String checkErrorCmd = SSH_COMMAND_BASE + action.getTrackerUri() + " ls " + outFile;
            int retVal = getReturnValue(checkErrorCmd);
            if (retVal == 0) {
                aStatus = Status.ERROR;
            }
            else {
                aStatus = Status.OK;
            }
        }
        return aStatus;
    }

    /**
     * Execute the callable.
     *
     * @param callable required callable.
     * @throws ActionExecutorException thrown if there is any error in command execution.
     */
    private <T> T execute(Callable<T> callable) throws ActionExecutorException {
        XLog log = XLog.getLog(getClass());
        try {
            return callable.call();
        }
        catch (IOException ex) {
            log.warn("Error while executing ssh EXECUTION");
            String errorMessage = ex.getMessage();
            if (null == errorMessage) { // Unknown IOException
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, ERR_UNKNOWN_ERROR, ex
                        .getMessage(), ex);
            } // Host Resolution Issues
            else {
                if (errorMessage.contains("Could not resolve hostname") ||
                        errorMessage.contains("service not known")) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_HOST_RESOLUTION, ex
                            .getMessage(), ex);
                } // Connection Timeout. Host temporarily down.
                else {
                    if (errorMessage.contains("timed out")) {
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_COULD_NOT_CONNECT,
                                                          ex.getMessage(), ex);
                    }// Local ssh-base or ssh-wrapper missing
                    else {
                        if (errorMessage.contains("Required Local file")) {
                            throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_FNF,
                                                              ex.getMessage(), ex); // local_FNF
                        }// Required oozie bash scripts missing, after the copy was
                        // successful
                        else {
                            if (errorMessage.contains("No such file or directory")
                                    && (errorMessage.contains("ssh-base") || errorMessage.contains("ssh-wrapper"))) {
                                throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_FNF,
                                                                  ex.getMessage(), ex); // remote
                                // FNF
                            } // Required application execution binary missing (either
                            // caught by ssh-wrapper
                            else {
                                if (errorMessage.contains("command not found")) {
                                    throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT, ERR_FNF, ex
                                            .getMessage(), ex); // remote
                                    // FNF
                                } // Permission denied while connecting
                                else {
                                    if (errorMessage.contains("Permission denied")) {
                                        throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT,
                                                ERR_AUTH_FAILED, ex.getMessage(), ex);
                                    } // Permission denied while executing
                                    else {
                                        if (errorMessage.contains(": Permission denied")) {
                                            throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT,
                                                    ERR_NO_EXEC_PERM, ex.getMessage(), ex);
                                        }
                                        else {
                                            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR,
                                                    ERR_UNKNOWN_ERROR, ex.getMessage(), ex);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } // Any other type of exception
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /**
     * Checks whether the system is configured to always use the oozie user for ssh, and injects the user if required.
     *
     * @param host the host string.
     * @param context the execution context.
     * @return the modified host string with a user parameter added on if required.
     * @throws ActionExecutorException in case the flag to use the oozie user is turned on and there is a mismatch
     * between the user specified in the host and the oozie user.
     */
    private String prepareUserHost(String host, Context context) throws ActionExecutorException {
        String oozieUser = context.getProtoActionConf().get(OozieClient.USER_NAME);
        if (allowSshUserAtHost) {
            if (!host.contains("@")) {
                host = oozieUser + "@" + host;
            }
        }
        else {
            if (host.contains("@")) {
                if (!host.toLowerCase().startsWith(oozieUser + "@")) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, ERR_USER_MISMATCH,
                                                      XLog.format("user mismatch between oozie user [{0}] and ssh host [{1}]",
                                                              oozieUser, host));
                }
            }
            else {
                host = oozieUser + "@" + host;
            }
        }
        return host;
    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return true;
    }

    /**
     * Truncate the string to max length.
     *
     * @param strBuffer
     * @return truncated string string
     */
    private String getTruncatedString(StringBuffer strBuffer) {

        if (strBuffer.length() <= maxLen) {
            return strBuffer.toString();
        }
        else {
            return strBuffer.substring(0, maxLen);
        }
    }

    /**
     * Drains the inputStream and errorStream of the Process being executed. The contents of the streams are stored if a
     * buffer is provided for the stream.
     *
     * @param p The Process instance.
     * @param inputBuffer The buffer into which STDOUT is to be read. Can be null if only draining is required.
     * @param errorBuffer The buffer into which STDERR is to be read. Can be null if only draining is required.
     * @param maxLength The maximum data length to be stored in these buffers. This is an indicative value, and the
     * store content may exceed this length.
     * @return the exit value of the process.
     * @throws IOException
     */
    private int drainBuffers(Process p, StringBuffer inputBuffer, StringBuffer errorBuffer, int maxLength)
            throws IOException {
        int exitValue = -1;
        BufferedReader ir = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader er = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        int inBytesRead = 0;
        int errBytesRead = 0;

        boolean processEnded = false;

        try {
            while (!processEnded) {
                try {
                    exitValue = p.exitValue();
                    processEnded = true;
                }
                catch (IllegalThreadStateException ex) {
                    // Continue to drain.
                }

                inBytesRead += drainBuffer(ir, inputBuffer, maxLength, inBytesRead, processEnded);
                errBytesRead += drainBuffer(er, errorBuffer, maxLength, errBytesRead, processEnded);
            }
        }
        finally {
            ir.close();
            er.close();
        }
        return exitValue;
    }

    /**
     * Reads the contents of a stream and stores them into the provided buffer.
     *
     * @param br The stream to be read.
     * @param storageBuf The buffer into which the contents of the stream are to be stored.
     * @param maxLength The maximum number of bytes to be stored in the buffer. An indicative value and may be
     * exceeded.
     * @param bytesRead The number of bytes read from this stream to date.
     * @param readAll If true, the stream is drained while their is data available in it. Otherwise, only a single chunk
     * of data is read, irrespective of how much is available.
     * @return
     * @throws IOException
     */
    private int drainBuffer(BufferedReader br, StringBuffer storageBuf, int maxLength, int bytesRead, boolean readAll)
            throws IOException {
        int bReadSession = 0;
        if (br.ready()) {
            char[] buf = new char[1024];
            do {
                int bReadCurrent = br.read(buf, 0, 1024);
                if (storageBuf != null && bytesRead < maxLength) {
                    storageBuf.append(buf, 0, bReadCurrent);
                }
                bReadSession += bReadCurrent;
            } while (br.ready() && readAll);
        }
        return bReadSession;
    }

    /**
     * Returns the first line from a StringBuffer, recognized by the new line character \n.
     *
     * @param buffer The StringBuffer from which the first line is required.
     * @return The first line of the buffer.
     */
    private String getFirstLine(StringBuffer buffer) {
        int newLineIndex = 0;
        newLineIndex = buffer.indexOf("\n");
        if (newLineIndex == -1) {
            return buffer.toString();
        }
        else {
            return buffer.substring(0, newLineIndex);
        }
    }
}