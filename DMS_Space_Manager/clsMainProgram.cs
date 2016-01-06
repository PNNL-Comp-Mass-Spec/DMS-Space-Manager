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

namespace Space_Manager
{
	class clsMainProgram
	{
		//*********************************************************************************************************
		// Main program execution loop for application
		//**********************************************************************************************************

		#region "Enums"
		private enum BroadcastCmdType
		{
			Shutdown,
			ReadConfig,
			Invalid
		}

		//private enum LoopExitCode
		//{
		//    NoTaskFound,
		//    ConfigChanged,
		//    ExceededMaxTaskCount,
		//    DisabledMC,
		//    DisabledLocally,
		//    ExcessiveErrors,
		//    InvalidWorkDir,
		//    ShutdownCmdReceived
		//}

		protected enum DriveOpStatus
		{
			KeepRunning,
			Exit_Restart_OK,
			Exit_No_Restart
		}
		#endregion

		#region "Constants"
		private const int MAX_ERROR_COUNT = 55; // Zero-based, so will give 56 tries
		private const bool RESTART_OK = true;
		private const bool RESTART_NOT_OK = false;
		#endregion

		#region "Class variables"
		private clsMgrSettings m_MgrSettings;
		private clsSpaceMgrTask m_Task;
		private FileSystemWatcher m_FileWatcher;
		private bool m_ConfigChanged;
		private int m_ErrorCount;
		private IStatusFile m_StatusFile;

		private clsMessageHandler m_MsgHandler;
		private bool m_MsgQueueInitSuccess;

		private string m_MgrName = "Unknown";
		private System.Timers.Timer m_StatusTimer;
		private readonly DateTime m_DurationStart = DateTime.UtcNow;
		private clsStorageOperations m_StorageOps;
		#endregion

		#region "Constructors"
		/// <summary>
		/// Constructor
		/// </summary>
		public clsMainProgram()
		{
			// Does nothing at present
		}
		#endregion

		#region "Methods"
		/// <summary>
		/// Initializes the manager
		/// </summary>
		/// <returns>TRUE for success; FALSE otherwise</returns>
		public bool InitMgr()
		{
			// Get the manager settings
			try
			{
				m_MgrSettings = new clsMgrSettings();
			}
			catch
			{
				// Failures are logged by clsMgrSettings
				return false;
			}

			// Update the cached manager name
			m_MgrName = m_MgrSettings.GetParam("MgrName");

			// Set up the loggers
			var logFileName = m_MgrSettings.GetParam("logfilename");
			var debugLevel = int.Parse(m_MgrSettings.GetParam("debuglevel"));
			clsLogTools.CreateFileLogger(logFileName, debugLevel);
			var logCnStr = m_MgrSettings.GetParam("connectionstring");

			clsLogTools.CreateDbLogger(logCnStr, "SpaceManager: " + m_MgrName);

			// Make initial log entry
			var msg = "=== Started Space Manager V" + System.Windows.Forms.Application.ProductVersion + " ===== ";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			// Setup the message queue
			m_MsgQueueInitSuccess = false;
			m_MsgHandler = new clsMessageHandler();
			m_MsgHandler.BrokerUri = m_MsgHandler.BrokerUri = m_MgrSettings.GetParam("MessageQueueURI");
			m_MsgHandler.CommandQueueName = m_MgrSettings.GetParam("ControlQueueName");
			m_MsgHandler.BroadcastTopicName = m_MgrSettings.GetParam("BroadcastQueueTopic");
			m_MsgHandler.StatusTopicName = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");
			m_MsgHandler.MgrSettings = m_MgrSettings;

			// Initialize the message queue
			// Start this in a separate thread so that we can abort the initialization if necessary
			InitializeMessageQueue();

			if (m_MsgQueueInitSuccess)
			{
				//Connect message handler events
				m_MsgHandler.CommandReceived += OnCommandReceived;
				m_MsgHandler.BroadcastReceived += OnBroadcastReceived;
			}

			// Setup a file watcher for the config file
			var fInfo = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
			m_FileWatcher = new FileSystemWatcher();
			m_FileWatcher.BeginInit();
			m_FileWatcher.Path = fInfo.DirectoryName;
			m_FileWatcher.IncludeSubdirectories = false;
			m_FileWatcher.Filter = m_MgrSettings.GetParam("configfilename");
			m_FileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
			m_FileWatcher.EndInit();
			m_FileWatcher.EnableRaisingEvents = true;

			// Subscribe to the file watcher Changed event
			m_FileWatcher.Changed += FileWatcherChanged;

			// Set up the tool for getting tasks
			m_Task = new clsSpaceMgrTask(m_MgrSettings);

			// Set up the status file class
			var statusFileNameLoc = Path.Combine(fInfo.DirectoryName, "Status.xml");
			m_StatusFile = new clsStatusFile(statusFileNameLoc)
			{
				//Note: Might want to put this back in someday
				//MonitorUpdateRequired += new StatusMonitorUpdateReceived(OnStatusMonitorUpdateReceived);
				LogToMsgQueue = bool.Parse(m_MgrSettings.GetParam("LogStatusToMessageQueue")),
				MgrName = m_MgrName,
				MgrStatus = EnumMgrStatus.Running
			};
			m_StatusFile.WriteStatusFile();

			// Set up the status reporting time
			m_StatusTimer = new System.Timers.Timer();
			m_StatusTimer.BeginInit();
			m_StatusTimer.Enabled = false;
			m_StatusTimer.Interval = 60000;	// 1 minute
			m_StatusTimer.EndInit();
			m_StatusTimer.Elapsed += m_StatusTimer_Elapsed;

			// Get the most recent job history
			var historyFile = Path.Combine(m_MgrSettings.GetParam("ApplicationPath"), "History.txt");
			if (File.Exists(historyFile))
			{
				try
				{
					// Create an instance of StreamReader to read from a file.
					// The using statement also closes the StreamReader.
					using (var sr = new StreamReader(historyFile))
					{
						String line;
						// Read and display lines from the file until the end of 
						// the file is reached.
						while ((line = sr.ReadLine()) != null)
						{
							if (line.Contains("RecentJob: "))
							{
								var tmpStr = line.Replace("RecentJob: ", "");
								m_StatusFile.MostRecentJobInfo = tmpStr;
								break;
							}
						}
					}
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
													"Exception readining status history file", ex);
				}
			}

			// Set up the storage operations class
			m_StorageOps = new clsStorageOperations(m_MgrSettings);

			// Everything worked!
			return true;
		}

		private void InitializeMessageQueue()
		{
            const int MAX_WAIT_TIME_SECONDS = 60;

			var worker = new System.Threading.Thread(InitializeMessageQueueWork);
			worker.Start();

		    var dtWaitStart = DateTime.UtcNow;

			// Wait a maximum of 60 seconds
		    if (!worker.Join(MAX_WAIT_TIME_SECONDS * 1000))
		    {
		        worker.Abort();
		        m_MsgQueueInitSuccess = false;
		        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
		                             "Unable to initialize the message queue (timeout after " + MAX_WAIT_TIME_SECONDS + " seconds)");
                return;
		    }
		    
            var elaspedTime = DateTime.UtcNow.Subtract(dtWaitStart).TotalSeconds;

		    if (elaspedTime > 25)
		    {
		        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
		                             "Connection to the message queue was slow, taking " + (int)elaspedTime + " seconds");
		    }
		}

		private void InitializeMessageQueueWork()
		{

			if (!m_MsgHandler.Init())
			{
				// Most error messages provided by .Init method, but debug message is here for program tracking
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message handler init error");
				m_MsgQueueInitSuccess = false;
			}
			else
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message handler initialized");
				m_MsgQueueInitSuccess = true;
			}

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
				var maxReps = int.Parse(m_MgrSettings.GetParam("maxrepetitions"));

				// Check if manager has been disabled via manager config db
				string msg;
				if (!string.Equals(m_MgrSettings.GetParam("mgractive"), "true", StringComparison.InvariantCultureIgnoreCase))
				{
					// Manager deactivated via manager config db
					msg = "Manager disabled via config db";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					msg = "===== Closing Space Manager =====";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					return RESTART_NOT_OK;
				}

				// Get a list of drives needing space management
				var driveList = clsUtilityMethods.GetDriveList(m_MgrSettings.GetParam("drives"));
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
					msg = "===== Closing Space Manager =====";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				else
				{
					// Program restart required
					msg = "Restarting manager";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in PerformSpaceManagement", ex);
			}

			return methodReturnCode;

		}

		protected DriveOpStatus ProcessDrive(int maxReps, clsDriveData testDrive)
		{
            const int MAX_MISSING_FOLDERS = 50;

			var opStatus = DriveOpStatus.KeepRunning;
			var repCounter = 0;
		    var folderMissingCount = 0;

			try
			{

				// Start a purge loop for the current drive
				var bDriveInfoLogged = false;
				while (true)
				{
					// Check for configuration changes
					string msg;
					if (m_ConfigChanged)
					{
						// Local config has changed, so exit loop and reload settings
						msg = "Local config changed. Reloading configuration";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						opStatus = DriveOpStatus.Exit_Restart_OK;
						break;
					}

					// Check to see if iteration limit has been exceeded
					if (repCounter >= maxReps)
					{
						// Exceeded max number of repetitions for this run, so exit
						msg = "Reached maximum repetition count of " + maxReps + "; Program exiting";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						opStatus = DriveOpStatus.Exit_No_Restart;
						break;
					}

				    if (folderMissingCount >= MAX_MISSING_FOLDERS)
				    {
                        // Too many missing folders; MyEMSL or the archive could be offline
                        msg = "Too many missing folders: MyEMSL or the archive could be offline; Program exiting";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
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
					double driveFreeSpaceGB;
					var serverName = m_MgrSettings.GetParam("machname");
					var perspective = m_MgrSettings.GetParam("perspective");
					var checkResult = clsUtilityMethods.IsPurgeRequired(serverName,
																		perspective,
																		testDrive,
																		out driveFreeSpaceGB);

					if (checkResult == SpaceCheckResults.Above_Threshold)
					{
						// Drive doesn't need purging, so continue to next drive
						msg = "No purge required, drive " + testDrive.DriveLetter + "; " + Math.Round(driveFreeSpaceGB, 0) + " GB free vs. " + Math.Round(testDrive.MinDriveSpace, 0) + " GB threshold";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						break;
					}

				    string pendingWindowsUpdateMessage;
				    if (PRISM.clsWindowsUpdateStatus.ServerUpdatesArePending(DateTime.Now, out pendingWindowsUpdateMessage))
				    {
                        msg = "Exiting: " + pendingWindowsUpdateMessage;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
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
						msg = "Purge required   , drive " + testDrive.DriveLetter + "; " + Math.Round(driveFreeSpaceGB, 0) + " GB free vs. " + Math.Round(testDrive.MinDriveSpace, 0) + " GB threshold";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
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
						msg = "Drive purge required, but no purge task assigned";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
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
						case EnumCloseOutType.CLOSEOUT_DATASET_FOLDER_MISSING_IN_ARCHIVE:
							repCounter++;
					        folderMissingCount++;
							break;
					}

					// Close the purge task
					m_Task.CloseTask(purgeResult);

					if (purgeResult == EnumCloseOutType.CLOSEOUT_DRIVE_MISSING)
					{
						msg = "Drive not found; moving on to next drive";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
						break;
					}

				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ProcessDrive", ex);
			}

			return opStatus;

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
				var msg = "Excessive errors. Error count = " + m_ErrorCount + ". Closing manager";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

				return false;
			}
			
			return true;
		}
		#endregion

		#region "Event handlers"
		/// <summary>
		/// Config file has been updated
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		private void FileWatcherChanged(object sender, FileSystemEventArgs e)
		{
			const string msg = "clsMainProgram.FileWatcherChanged event received";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			m_ConfigChanged = true;
			m_FileWatcher.EnableRaisingEvents = false;
		}

		/// <summary>
		/// Handles received manager control command
		/// </summary>
		/// <param name="cmdText"></param>
		private void OnBroadcastReceived(string cmdText)
		{
			var msg = "clsMainProgram.OnBroadcasetReceived event; message = " + cmdText;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			clsBroadcastCmd recvCmd;

			// Parse the received message
			try
			{
				recvCmd = clsXMLTools.ParseBroadcastXML(cmdText);
			}
			catch (Exception Ex)
			{
				msg = "Exception while parsing broadcast data";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, Ex);
				return;
			}

			// Determine if the message applies to this machine
			if (!recvCmd.MachineList.Contains(m_MgrName))
			{
				// Received command doesn't apply to this manager
				msg = "Received command not applicable to this manager instance";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				return;
			}

			// Get the command and take appropriate action
			switch (recvCmd.MachCmd.ToLower())
			{
				case "shutdown":
					//     m_LoopExitCode = LoopExitCode.ShutdownCmdReceived;
					//	   m_Running = false;
					break;
				case "readconfig":
					msg = "Reload config message received";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					m_ConfigChanged = true;
					//	   m_Running = false;
					break;
				default:
					// Invalid command received; do nothing except log it
					msg = "Invalid broadcast command received: " + cmdText;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
					break;
			}
		}

		/// <summary>
		/// Handles manager execution command (future)
		/// </summary>
		/// <param name="cmdText"></param>
		private void OnCommandReceived(string cmdText)
		{
			//TODO: (Future)
		}

		/// <summary>
		/// Updates the status at m_StatusTimer interval
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		void m_StatusTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
		{
			var duration = DateTime.UtcNow - m_DurationStart;
			m_StatusFile.Duration = (Single)duration.TotalHours;
			m_StatusFile.WriteStatusFile();
		}
		#endregion
	}
}
