//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Management;
using System.Reflection;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMDatabaseUtils;
using PRISMDatabaseUtils.AppSettings;

namespace Space_Manager
{
    /// <summary>
    /// Main program execution loop for application
    /// </summary>
    internal class MainProgram : LoggerBase
    {
        private enum DriveOpStatus
        {
            KeepRunning,
            Exit_Restart_OK,
            Exit_No_Restart
        }

        private const string DEFAULT_BASE_LOGFILE_NAME = @"Logs\SpaceMan";

        private const string MGR_PARAM_DEFAULT_DMS_CONN_STRING = "DefaultDMSConnString";

        private const int MAX_ERROR_COUNT = 55; // Zero-based, so will give 56 tries

        private const bool RESTART_OK = true;

        private const bool RESTART_NOT_OK = false;

        private MgrSettings m_MgrSettings;
        private SpaceMgrTask m_Task;
        private FileSystemWatcher m_FileWatcher;
        private bool m_ConfigChanged;
        private int m_ErrorCount;
        private IStatusFile m_StatusFile;

        private string m_MgrName = "Unknown";

        /// <summary>
        /// DebugLevel of 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
        /// </summary>
        private int m_DebugLevel = 4;

        private System.Timers.Timer m_StatusTimer;
        private StorageOperations m_StorageOps;

        public bool PreviewMode { get; }

        public bool TraceMode { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MainProgram(bool previewMode = false, bool traceMode = false)
        {
            PreviewMode = previewMode;
            TraceMode = traceMode;

            if (PreviewMode)
                Console.WriteLine("Preview mode enabled");

            if (TraceMode)
                Console.WriteLine("Trace mode enabled");
        }

        /// <summary>
        /// Use the hidden admin share to determine the free space of a drive on a remote computer
        /// </summary>
        /// <param name="remoteServer"></param>
        /// <param name="driveData"></param>
        /// <param name="driveFreeSpaceGB"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool GetFreeSpaceUsingAdminShare(string remoteServer, DriveData driveData, out double driveFreeSpaceGB)
        {
            var postToDB = !Environment.MachineName.StartsWith("WE43320", StringComparison.OrdinalIgnoreCase);

            // This path must end with a backslash
            var sharePath = string.Format(@"\\{0}\\{1}$\\", remoteServer, driveData.DriveLetter.Substring(0, 1));

            driveFreeSpaceGB = 0;

            try
            {
                var remoteShare = new DirectoryInfo(sharePath);

                long freeBytesAvailable = 0;
                long totalNumberOfBytes = 0;
                long totalNumberOfFreeBytes = 0;

                if (RemoteShareInfo.GetDiskFreeSpaceEx(
                        remoteShare.FullName,
                        ref freeBytesAvailable,
                        ref totalNumberOfBytes,
                        ref totalNumberOfFreeBytes))
                {
                    driveFreeSpaceGB = BytesToGB(freeBytesAvailable);
                    return true;
                }

                LogError("GetDiskFreeSpaceEx returned false for " + sharePath, postToDB);
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception getting free disk space using GetDiskFreeSpaceEx, share path " + sharePath + ": " + ex.Message;

                LogError(msg, postToDB);

                if (driveFreeSpaceGB > 0)
                    driveFreeSpaceGB = -driveFreeSpaceGB;

                if (Math.Abs(driveFreeSpaceGB) < float.Epsilon)
                    driveFreeSpaceGB = -1;

                return false;
            }
        }

        /// <summary>
        /// Use WMI to determine the free space of a drive on a remote computer
        /// </summary>
        /// <param name="remoteServer"></param>
        /// <param name="driveData"></param>
        /// <param name="driveFreeSpaceGB"></param>
        /// <returns>True if successful, false if an error</returns>
        // ReSharper disable once MemberCanBeMadeStatic.Local
        private bool GetFreeSpaceUsingWMI(string remoteServer, DriveData driveData, out double driveFreeSpaceGB)
        {
            var postToDB = !Environment.MachineName.StartsWith("WE43320", StringComparison.OrdinalIgnoreCase);

            // Get WMI object representing drive
            var requestStr = @"\\" + remoteServer + @"\root\cimv2:win32_logicaldisk.deviceid=""" + driveData.DriveLetter + "\"";

            driveFreeSpaceGB = 0;

            try
            {
                var disk = new ManagementObject(requestStr);
                disk.Get();

                var freeSpaceBytes = disk["FreeSpace"];
                if (freeSpaceBytes == null)
                {
                    LogError("Drive " + driveData.DriveLetter + " not found via WMI; likely is Not Ready", true);
                    {
                        return false;
                    }
                }

                var availableSpace = Convert.ToDouble(freeSpaceBytes);
                var totalSpace = Convert.ToDouble(disk["Size"]);

                if (totalSpace <= 0)
                {
                    LogError("Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via WMI; likely is Not Ready", postToDB);
                    {
                        return false;
                    }
                }

                driveFreeSpaceGB = BytesToGB((long)availableSpace);

                return true;
            }
            catch (Exception ex)
            {
                var msg = "Exception getting free disk space using WMI, drive " + driveData.DriveLetter + ": " + ex.Message;

                LogError(msg, postToDB);

                if (driveFreeSpaceGB > 0)
                    driveFreeSpaceGB = -driveFreeSpaceGB;

                if (Math.Abs(driveFreeSpaceGB) < float.Epsilon)
                    driveFreeSpaceGB = -1;

                return false;
            }
        }

        /// <summary>
        /// Initializes the manager
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool InitMgr()
        {
            // Define the default logging info
            // This will get updated below
            LogTools.CreateFileLogger(DEFAULT_BASE_LOGFILE_NAME, BaseLogger.LogLevels.DEBUG);

            // Create a database logger connected to DMS5
            // Once the initial parameters have been successfully read,
            // we remove this logger than make a new one using the connection string read from the Manager Control DB
            var defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;

            var applicationName = "SpaceManager_" + System.Net.Dns.GetHostName();
            var defaultDbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(defaultDmsConnectionString, applicationName);

            LogTools.CreateDbLogger(defaultDbLoggerConnectionString, "SpaceManager: " + System.Net.Dns.GetHostName());

            // Get the manager settings
            // If you get an exception here while debugging in Visual Studio, be sure
            //  that "UsingDefaults" is set to False in CaptureTaskManager.exe.config
            try
            {
                var defaultSettings = new Dictionary<string, string>
                {
                    {MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING, Properties.Settings.Default.MgrCnfgDbConnectStr},
                    {MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, Properties.Settings.Default.MgrActive_Local.ToString()},
                    {MgrSettings.MGR_PARAM_MGR_NAME, Properties.Settings.Default.MgrName},
                    {MgrSettings.MGR_PARAM_USING_DEFAULTS, Properties.Settings.Default.UsingDefaults.ToString()},
                    {MGR_PARAM_DEFAULT_DMS_CONN_STRING, Properties.Settings.Default.DefaultDMSConnString},
                };

                m_MgrSettings = new MgrSettingsDB();
                RegisterEvents(m_MgrSettings);
                m_MgrSettings.CriticalErrorEvent += ErrorEventHandler;

                var mgrExePath = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath();
                var localSettings = m_MgrSettings.LoadMgrSettingsFromFile(mgrExePath + ".config");

                if (localSettings == null)
                {
                    localSettings = defaultSettings;
                }
                else
                {
                    // Make sure the default settings exist and have valid values
                    foreach (var setting in defaultSettings)
                    {
                        if (!localSettings.TryGetValue(setting.Key, out var existingValue) ||
                            string.IsNullOrWhiteSpace(existingValue))
                        {
                            localSettings[setting.Key] = setting.Value;
                        }
                    }
                }

                var success = m_MgrSettings.LoadSettings(localSettings, true);
                if (!success)
                {
                    if (string.Equals(m_MgrSettings.ErrMsg, MgrSettings.DEACTIVATED_LOCALLY))
                        throw new ApplicationException(MgrSettings.DEACTIVATED_LOCALLY);

                    throw new ApplicationException("Unable to initialize manager settings class: " + m_MgrSettings.ErrMsg);
                }

                ReportStatus("Loaded manager settings from Manager Control Database");
            }
            catch
            {
                // Failures are logged by MgrSettings
                return false;
            }

            // Update the cached manager name
            m_MgrName = m_MgrSettings.ManagerName;

            // Set up the loggers
            var logFileNameBase = m_MgrSettings.GetParam("LogFileName", "SpaceMan");

            // LogLevel is 1 to 5: 1 for Fatal errors only, 4 for Fatal, Error, Warning, and Info, and 5 for everything including Debug messages
            m_DebugLevel = m_MgrSettings.GetParam("DebugLevel", 4);

            var logLevel = (BaseLogger.LogLevels)m_DebugLevel;

            LogTools.CreateFileLogger(logFileNameBase, logLevel);

            // Typically
            // Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;
            var connectionString = m_MgrSettings.GetParam("ConnectionString");

            var dbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, m_MgrName);

            LogTools.RemoveDefaultDbLogger();
            LogTools.CreateDbLogger(dbLoggerConnectionString, "SpaceManager: " + m_MgrName);

            // Make the initial log entry
            var relativeLogFilePath = LogTools.CurrentLogFilePath;
            var logFile = new FileInfo(relativeLogFilePath);
            ConsoleMsgUtils.ShowDebug("Initializing log file " + PathUtils.CompactPathString(logFile.FullName, 60));

            var appVersion = Assembly.GetEntryAssembly()?.GetName().Version;
            ReportStatus("=== Started Space Manager V" + appVersion + " ===== ");

            // Setup the message queue
            // NOT USED; if changing to use, can copy MessageSender class from CaptureTaskManager:
            // mMsgHandler = new MessageSender(m_MgrSettings.GetParam("MessageQueueURI"),
            //     m_MgrSettings.GetParam("MessageQueueTopicMgrStatus"),
            //     m_MgrSettings.ManagerName);

            var configFileName = m_MgrSettings.GetParam("ConfigFileName");
            if (string.IsNullOrEmpty(configFileName))
            {
                // Manager parameter error; log an error and exit
                LogError("Manager parameter 'ConfigFileName' is undefined; this likely indicates a problem retrieving manager parameters. " +
                         "Shutting down the manager");
                return false;
            }

            // Setup a file watcher for the config file
            var appPath = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath();
            var fInfo = new FileInfo(appPath);
            m_FileWatcher = new FileSystemWatcher
            {
                Path = fInfo.DirectoryName,
                IncludeSubdirectories = false,
                Filter = configFileName,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };

            // Subscribe to the file watcher Changed event
            m_FileWatcher.Changed += FileWatcherChanged;

            // Set up the tool for getting tasks
            m_Task = new SpaceMgrTask(m_MgrSettings, TraceMode);

            // Set up the status file class
            if (fInfo.DirectoryName == null)
            {
                LogError("Error determining the parent path for the executable, " + appPath);
                return false;
            }

            var statusFileNameLoc = Path.Combine(fInfo.DirectoryName, "Status.xml");
            m_StatusFile = new StatusFile(statusFileNameLoc)
            {
                MgrName = m_MgrName,
                MgrStatus = EnumMgrStatus.Running
            };

            RegisterEvents((EventNotifier)m_StatusFile);

            var logStatusToMessageQueue = m_MgrSettings.GetParam("LogStatusToMessageQueue", true);
            var messageQueueUri = m_MgrSettings.GetParam("MessageQueueURI");
            var messageQueueTopicMgrStatus = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");

            m_StatusFile.ConfigureMessageQueueLogging(logStatusToMessageQueue, messageQueueUri, messageQueueTopicMgrStatus);

            m_StatusFile.WriteStatusFile();

            // Set up the status reporting time
            m_StatusTimer = new System.Timers.Timer
            {
                Enabled = false,
                Interval = 60 * 1000
            };
            m_StatusTimer.Elapsed += StatusTimer_Elapsed;

            // Get the most recent job history
            var historyFilePath = Path.Combine(m_MgrSettings.GetParam("ApplicationPath"), "History.txt");

            if (File.Exists(historyFilePath))
            {
                try
                {
                    // Create an instance of StreamReader to read from a file.
                    using var reader = new StreamReader(new FileStream(historyFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                    // Read and display lines from the file until the end of
                    // the file is reached.
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.Contains("RecentJob: "))
                        {
                            m_StatusFile.MostRecentJobInfo = dataLine.Replace("RecentJob: ", string.Empty);
                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception reading status history file", ex);
                }
            }

            // Set up the storage operations class
            m_StorageOps = new StorageOperations(m_MgrSettings)
            {
                PreviewMode = PreviewMode,
                TraceMode = TraceMode
            };

            // Everything worked!
            return true;
        }

        /// <summary>
        /// Performs space management for all drives on specified server
        /// </summary>
        /// <returns>Exit code specifying manager stop or restart</returns>
        public bool PerformSpaceManagement()
        {
            var methodReturnCode = RESTART_NOT_OK;

            try
            {
                var maxReps = m_MgrSettings.GetParam("MaxRepetitions", 25);

                // Check if manager has been disabled via manager config db
                if (!m_MgrSettings.GetParam("MgrActive", false))
                {
                    // Manager deactivated via manager config db
                    ReportStatus("Manager disabled via config db");
                    LogTools.LogMessage("===== Closing Space Manager =====");
                    return RESTART_NOT_OK;
                }

                // Get a list of drives needing space management
                var driveList = GetDriveList(m_MgrSettings.GetParam("drives"));
                if (driveList == null)
                {
                    // Problem with drive spec. Error reporting handled by GetDriveList
                    return RESTART_NOT_OK;
                }

                // Set drive operation state to Keep Running
                var opStatus = DriveOpStatus.KeepRunning;

                foreach (var testDrive in driveList)
                {
                    // Check drive operation state
                    if (opStatus != DriveOpStatus.KeepRunning)
                    {
                        if (opStatus == DriveOpStatus.Exit_No_Restart)
                        {
                            // Something has happened that requires restarting manager
                            methodReturnCode = RESTART_NOT_OK;
                        }
                        else
                        {
                            methodReturnCode = RESTART_OK;	// Something is requiring a manager restart
                        }
                        // Exit the drive test loop
                        break;
                    }

                    opStatus = ProcessDrive(maxReps, testDrive);
                }	// End drive loop

                // Set status and exit method
                if (methodReturnCode == RESTART_NOT_OK)
                {
                    // Program exit required
                    LogTools.LogMessage("===== Closing Space Manager =====");
                }
                else
                {
                    // Program restart required
                    LogTools.LogMessage("Restarting manager");
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in PerformSpaceManagement", ex);
            }

            return methodReturnCode;
        }

        private DriveOpStatus ProcessDrive(int maxReps, DriveData testDrive)
        {
            const int MAX_MISSING_DIRECTORIES = 50;

            var opStatus = DriveOpStatus.KeepRunning;
            var repCounter = 0;
            var directoryMissingCount = 0;

            try
            {
                // Start a purge loop for the current drive
                var bDriveInfoLogged = false;
                while (true)
                {
                    // Check for configuration changes
                    if (m_ConfigChanged)
                    {
                        // Local config has changed, so exit loop and reload settings
                        ReportStatus("Local config changed. Reloading configuration");
                        opStatus = DriveOpStatus.Exit_Restart_OK;
                        break;
                    }

                    // Check to see if iteration limit has been exceeded
                    if (repCounter >= maxReps)
                    {
                        // Exceeded max number of repetitions for this run, so exit
                        ReportStatus("Reached maximum repetition count of " + maxReps + "; Program exiting");
                        opStatus = DriveOpStatus.Exit_No_Restart;
                        break;
                    }

                    if (directoryMissingCount >= MAX_MISSING_DIRECTORIES)
                    {
                        // Too many missing directories; MyEMSL or the archive could be offline
                        LogError("Too many missing directories: MyEMSL or the archive could be offline; Program exiting");
                        opStatus = DriveOpStatus.Exit_No_Restart;
                        break;
                    }

                    // Check error count
                    if (!TestErrorCount())
                    {
                        // Excessive errors. Program exit required. Logging handled by TestErrorCount
                        opStatus = DriveOpStatus.Exit_No_Restart;
                        break;
                    }

                    // Check available space on server drive and compare it with min allowed space
                    var serverName = m_MgrSettings.GetParam("MachName");
                    var perspective = m_MgrSettings.GetParam("perspective");
                    var checkResult = IsPurgeRequired(serverName,
                                                                        perspective,
                                                                        testDrive,
                                                                        out var driveFreeSpaceGB);

                    if (checkResult == SpaceCheckResults.Above_Threshold)
                    {
                        // Drive doesn't need purging, so continue to next drive
                        ReportStatus("No purge required, drive " + testDrive.DriveLetter + " " + Math.Round(driveFreeSpaceGB, 0) + " GB free vs. " + Math.Round(testDrive.MinDriveSpace, 0) + " GB threshold");
                        break;
                    }

                    if (WindowsUpdateStatus.ServerUpdatesArePending(DateTime.Now, out var pendingWindowsUpdateMessage))
                    {
                        ReportStatus("Exiting: " + pendingWindowsUpdateMessage);
                        break;
                    }

                    if (checkResult == SpaceCheckResults.Error)
                    {
                        // There was an error getting the free space for this drive. Logging handled by IsPurgeRequired
                        m_ErrorCount++;
                        break;
                    }

                    if (!bDriveInfoLogged)
                    {
                        bDriveInfoLogged = true;
                        // Note: there are extra spaces after "required" so the log message lines up with the "No purge required" message
                        ReportStatus("Purge required   , drive " + testDrive.DriveLetter + " " + Math.Round(driveFreeSpaceGB, 0) + " GB free vs. " + Math.Round(testDrive.MinDriveSpace, 0) + " GB threshold");
                    }

                    // Request a purge task
                    var requestResult = m_Task.RequestTask(testDrive.DriveLetter);

                    // Check for an error
                    if (requestResult == EnumRequestTaskResult.ResultError)
                    {
                        // Error requesting task. Error logging handled by RequestTask, so just continue to next purge candidate
                        m_ErrorCount++;
                        repCounter++;
                        continue;
                    }

                    // Check for MC database config change
                    if (requestResult == EnumRequestTaskResult.ConfigChanged)
                    {
                        // Manager control db has changed. Set flag and allow config test at beginning of loop to control restart
                        m_ConfigChanged = true;
                        continue;
                    }

                    // Check for task not assigned
                    if (requestResult == EnumRequestTaskResult.NoTaskFound)
                    {
                        // No purge task assigned. This is a problem because the drive is low on space
                        LogWarning("Drive purge required, but no purge task assigned");
                        break;
                    }

                    // If we got to here, the drive needs purging and a purge task was assigned. So, perform the purge
                    var purgeResult = m_StorageOps.PurgeDataset(m_Task);

                    // Evaluate purge result
                    switch (purgeResult)
                    {
                        case EnumCloseOutType.CLOSEOUT_SUCCESS:
                        case EnumCloseOutType.CLOSEOUT_PURGE_AUTO:
                        case EnumCloseOutType.CLOSEOUT_PURGE_ALL_EXCEPT_QC:
                            repCounter++;
                            m_ErrorCount = 0;
                            break;
                        case EnumCloseOutType.CLOSEOUT_UPDATE_REQUIRED:
                            repCounter++;
                            m_ErrorCount = 0;
                            break;
                        case EnumCloseOutType.CLOSEOUT_FAILED:
                            m_ErrorCount++;
                            repCounter++;
                            break;
                        // Obsolete:
                        //case EnumCloseOutType.CLOSEOUT_WAITING_HASH_FILE:
                        //	repCounter++;
                        //	m_ErrorCount = 0;
                        //	break;
                        case EnumCloseOutType.CLOSEOUT_DRIVE_MISSING:
                        case EnumCloseOutType.CLOSEOUT_DATASET_DIRECTORY_MISSING_IN_ARCHIVE:
                            repCounter++;
                            directoryMissingCount++;
                            break;
                    }

                    var simulateMode = false;
#if !DoDelete
                    simulateMode = true;
#endif
                    // Close the purge task
                    if (PreviewMode || simulateMode)
                        m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_PREVIEWED_PURGE);
                    else
                        m_Task.CloseTask(purgeResult);

                    if (purgeResult == EnumCloseOutType.CLOSEOUT_DRIVE_MISSING)
                    {
                        LogWarning("Drive not found; moving on to next drive");
                        break;
                    }

                    if (purgeResult == EnumCloseOutType.CLOSEOUT_ARCHIVE_OFFLINE)
                    {
                        LogWarning("Archive is offline; closing the manager");
                        opStatus = DriveOpStatus.Exit_No_Restart;
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ProcessDrive", ex);
            }

            return opStatus;
        }

        /// <summary>
        /// Parses a list of drive data objects from a string
        /// </summary>
        /// <param name="inpList">Input string containing drive information</param>
        /// <returns>List of drives with associated data</returns>
        private IEnumerable<DriveData> GetDriveList(string inpList)
        {
            if (string.IsNullOrWhiteSpace(inpList))
            {
                // There were no drives in string
                LogError("Drive list provided to GetDriveList is empty");
                return null;
            }

            // Data for drives is separated by semicolons
            var driveArray = inpList.Split(';');

            var driveList = new List<DriveData>();

            // Data for an individual drive is separated by comma
            foreach (var driveSpec in driveArray)
            {
                if (string.IsNullOrWhiteSpace(driveSpec))
                {
                    LogError("Unable to get drive space threshold from string, should be something like G:,600 and not " + driveSpec);
                    return null;
                }

                var driveInfo = driveSpec.Split(',');

                if (driveInfo.Length != 2)
                {
                    LogError("Invalid parameter count for drive data string " + driveSpec + ", should be something like G:,600");
                    return null;
                }

                // Add the data for this drive to the return list
                // Note that driveInfo[0] can be either just a drive letter or a drive letter and a colon; either is supported
                var newDrive = new DriveData(driveInfo[0], double.Parse(driveInfo[1]));
                driveList.Add(newDrive);
            }

            return driveList;
        }

        /// <summary>
        /// Convert bytes to Gigabytes
        /// </summary>
        /// <param name="bytes"></param>
        public static double BytesToGB(long bytes)
        {
            return bytes / 1024.0 / 1024.0 / 1024.0;
        }

        /// <summary>
        /// For remote drives, uses WMI to determine if free space on disk is above minimum threshold
        /// For local drives, uses DriveInfo
        /// </summary>
        /// <param name="machine">Name of server to check</param>
        /// <param name="perspective">Client/Server setting for manager.  "Client" means checking a remote drive; "Server" means running on a Proto-x server </param>
        /// <param name="driveData">Data for drive to be checked</param>
        /// <param name="driveFreeSpaceGB">Actual drive free space in GB</param>
        /// <returns>Enum indicating space status</returns>
        private SpaceCheckResults IsPurgeRequired(string machine, string perspective, DriveData driveData, out double driveFreeSpaceGB)
        {
            SpaceCheckResults testResult;

            driveFreeSpaceGB = -1;

            if (perspective.StartsWith("client", StringComparison.OrdinalIgnoreCase))
            {
                // Checking a remote drive

                // Option 1: use WMI
                // Option 2: use the hidden share (F$, G$, etc.)

                // WMI worked in 2018 from a developer computer but is not working in 2022
                // Thus, always use the hidden share method when accessing a remote computer

                const bool USE_WMI = false;

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (USE_WMI)
                {
                    if (!GetFreeSpaceUsingWMI(machine, driveData, out driveFreeSpaceGB))
                    {
                        return SpaceCheckResults.Error;
                    }
                }
                else
                {
                    if (!GetFreeSpaceUsingAdminShare(machine, driveData, out driveFreeSpaceGB))
                    {
                        return SpaceCheckResults.Error;
                    }
                }
            }
            else
            {
                // Analyzing a drive local to this manager

                try
                {
                    // Note: WMI string would be: "win32_logicaldisk.deviceid=\"" + driveData.DriveLetter + "\"";
                    // Instantiate a new drive info object
                    var diDrive = new DriveInfo(driveData.DriveLetter);

                    if (!diDrive.IsReady)
                    {
                        LogError("Drive " + driveData.DriveLetter + " reports Not Ready via DriveInfo object; drive is offline or drive letter is invalid", true);
                        return SpaceCheckResults.Error;
                    }

                    if (diDrive.TotalSize <= 0)
                    {
                        LogError("Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via DriveInfo object; likely is Not Ready", true);
                        return SpaceCheckResults.Error;
                    }

                    driveFreeSpaceGB = BytesToGB(diDrive.TotalFreeSpace);
                }
                catch (Exception ex)
                {
                    LogError("Exception getting free disk space via .NET DriveInfo object, drive " + driveData.DriveLetter + ": " + ex.Message, true);

                    if (driveFreeSpaceGB > 0)
                        driveFreeSpaceGB = -driveFreeSpaceGB;
                    if (Math.Abs(driveFreeSpaceGB) < float.Epsilon)
                        driveFreeSpaceGB = -1;
                }
            }

            if (driveFreeSpaceGB < 0)
            {
                testResult = SpaceCheckResults.Error;

                // Log space requirement if debug logging enabled
                ReportDebug("Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace + ", Drive not found");
            }
            else
            {
                if (driveFreeSpaceGB > driveData.MinDriveSpace)
                    testResult = SpaceCheckResults.Above_Threshold;
                else
                    testResult = SpaceCheckResults.Below_Threshold;

                // Log space requirement if debug logging enabled
                ReportStatus("Drive " + driveData.DriveLetter +
                    " Space Threshold: " + driveData.MinDriveSpace +
                    ", Avail space: " + driveFreeSpaceGB.ToString("####0.0"), true);
            }

            return testResult;
        }

        /// <summary>
        /// Checks for excessive errors
        /// </summary>
        /// <returns>TRUE if error count less than max allowed; FALSE otherwise</returns>
        private bool TestErrorCount()
        {
            if (m_ErrorCount > MAX_ERROR_COUNT)
            {
                // Too many errors - something must be seriously wrong. Human intervention may be required
                LogError("Excessive errors. Error count = " + m_ErrorCount + ". Closing manager");

                return false;
            }

            return true;
        }

        private void RegisterEvents(IEventNotifier oProcessingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                oProcessingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                oProcessingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            oProcessingClass.StatusEvent += StatusEventHandler;
            oProcessingClass.ErrorEvent += ErrorEventHandler;
            oProcessingClass.WarningEvent += WarningEventHandler;
            oProcessingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogDebug(statusMessage, writeToLog: false);
        }

        private void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        private void StatusEventHandler(string statusMessage)
        {
            ReportStatus(statusMessage);
        }

        private void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }

        private void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            m_StatusFile.CurrentOperation = progressMessage;
            m_StatusFile.UpdateAndWrite(percentComplete);
        }

        /// <summary>
        /// Config file has been updated
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void FileWatcherChanged(object sender, FileSystemEventArgs e)
        {
            LogDebug("MainProgram.FileWatcherChanged event received");

            m_ConfigChanged = true;
            m_FileWatcher.EnableRaisingEvents = false;
        }

        /// <summary>
        /// Updates the status at m_StatusTimer interval
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void StatusTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            m_StatusFile.WriteStatusFile();
        }
    }
}
