//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/08/2010
//
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using log4net;
using System.Data;

// Configure log4net using the .log4net file
using log4net.Appender;

[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Logging.config", Watch = true)]

namespace Space_Manager
{
	public class clsLogTools
	{
		//*********************************************************************************************************
		// Wraps Log4Net functions
		//**********************************************************************************************************

		#region "Enums"
			public enum LogLevels
			{
				DEBUG = 5,
				INFO = 4,
				WARN = 3,
				ERROR = 2,
				FATAL = 1
			}

			public enum LoggerTypes
			{
				LogFile,
				LogDb,
				LogSystem
			}
		#endregion

		#region "Class variables"
			private static readonly ILog m_FileLogger = LogManager.GetLogger("FileLogger");
			private static readonly ILog m_DbLogger = LogManager.GetLogger("DbLogger");
			private static readonly ILog m_SysLogger = LogManager.GetLogger("SysLogger");
			private static string m_FileDate;
			private static string m_BaseFileName;
			private static log4net.Appender.FileAppender m_FileAppender;
		#endregion

		#region "Properties"
			public static bool FileLogDebugEnabled
			{
				get { return m_FileLogger.IsDebugEnabled; }
			}
		#endregion

		#region "Methods"
			/// <summary>
			/// Writes a message to the logging system
			/// </summary>
			/// <param name="LoggerType">Type of logger to use</param>
			/// <param name="LogLevel">Level of log reporting</param>
			/// <param name="InpMsg">Message to be logged</param>
			public static void WriteLog(LoggerTypes LoggerType, LogLevels LogLevel, string InpMsg)
			{
				ILog MyLogger;

				//Establish which logger will be used
				switch (LoggerType)
				{
					case LoggerTypes.LogDb:
						MyLogger = m_DbLogger;
						break;
					case LoggerTypes.LogFile:
						MyLogger = m_FileLogger;
						// Check to determine if a new file should be started
						string TestFileDate = DateTime.Now.ToString("MM-dd-yyyy");
						if (TestFileDate != m_FileDate)
						{
							m_FileDate = TestFileDate;
							ChangeLogFileName();
						}
						break;
					case LoggerTypes.LogSystem:
						MyLogger = m_SysLogger;
						break;
					default:
						throw new Exception("Invalid logger type specified");
				}

				//Update the status file data
				clsStatusData.MostRecentLogMessage = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "; "
						+ InpMsg + "; " + LogLevel.ToString();

				//Send the log message
				switch (LogLevel)
				{
					case LogLevels.DEBUG:
						if (MyLogger.IsDebugEnabled) MyLogger.Debug(InpMsg);
						break;
					case LogLevels.ERROR:
						clsStatusData.AddErrorMessage(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "; " + InpMsg
								+ "; " + LogLevel.ToString());
						if (MyLogger.IsErrorEnabled) MyLogger.Error(InpMsg);
						break;
					case LogLevels.FATAL:
						if (MyLogger.IsFatalEnabled) MyLogger.Fatal(InpMsg);
						break;
					case LogLevels.INFO:
						if (MyLogger.IsInfoEnabled) MyLogger.Info(InpMsg);
						break;
					case LogLevels.WARN:
						if (MyLogger.IsWarnEnabled) MyLogger.Warn(InpMsg);
						break;
					default:
						throw new Exception("Invalid log level specified");
				}
			}

			/// <summary>
			/// Overload to write a message and exception to the logging system
			/// </summary>
			/// <param name="LoggerType">Type of logger to use</param>
			/// <param name="LogLevel">Level of log reporting</param>
			/// <param name="InpMsg">Message to be logged</param>
			/// <param name="Ex">Exception to be logged</param>
			public static void WriteLog(LoggerTypes LoggerType, LogLevels LogLevel, string InpMsg, Exception Ex)
			{
				ILog MyLogger;

				//Establish which logger will be used
				switch (LoggerType)
				{
					case LoggerTypes.LogDb:
						MyLogger = m_DbLogger;
						break;
					case LoggerTypes.LogFile:
						MyLogger = m_FileLogger;
						// Check to determine if a new file should be started
						string TestFileDate = DateTime.Now.ToString("MM-dd-yyyy");
						if (TestFileDate != m_FileDate)
						{
							m_FileDate = TestFileDate;
							ChangeLogFileName();
						}
						break;
					case LoggerTypes.LogSystem:
						MyLogger = m_SysLogger;
						break;
					default:
						throw new Exception("Invalid logger type specified");
				}

				//Update the status file data
				clsStatusData.MostRecentLogMessage = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "; "
						+ InpMsg + "; " + LogLevel.ToString();

				//Send the log message
				switch (LogLevel)
				{
					case LogLevels.DEBUG:
						if (MyLogger.IsDebugEnabled) MyLogger.Debug(InpMsg, Ex);
						break;
					case LogLevels.ERROR:
						clsStatusData.AddErrorMessage(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "; " + InpMsg
							 + "; " + LogLevel.ToString());

						if (MyLogger.IsErrorEnabled) MyLogger.Error(InpMsg, Ex);
						break;
					case LogLevels.FATAL:
						if (MyLogger.IsFatalEnabled) MyLogger.Fatal(InpMsg, Ex);
						break;
					case LogLevels.INFO:
						if (MyLogger.IsInfoEnabled) MyLogger.Info(InpMsg, Ex);
						break;
					case LogLevels.WARN:
						if (MyLogger.IsWarnEnabled) MyLogger.Warn(InpMsg, Ex);
						break;
					default:
						throw new Exception("Invalid log level specified");
				}
			}

			/// <summary>
			/// Changes the base log file name
			/// </summary>
			public static void ChangeLogFileName()
			{
				//Get a list of appenders
				IEnumerable<IAppender> AppendList = FindAppenders("FileAppender");
				if (AppendList == null)
				{
					WriteLog(LoggerTypes.LogSystem, LogLevels.WARN, "Unable to change file name. No appender found");
					return;
				}

				foreach (log4net.Appender.IAppender SelectedAppender in AppendList)
				{
					//Convert the IAppender object to a FileAppender
					var AppenderToChange = SelectedAppender as log4net.Appender.FileAppender;
					if (AppenderToChange == null)
					{
						WriteLog(LoggerTypes.LogSystem, LogLevels.ERROR, "Unable to convert appender");
						return;
					}
					//Change the file name and activate change
					AppenderToChange.File = m_BaseFileName + "_" + m_FileDate + ".txt";
					AppenderToChange.ActivateOptions();
				}
			}

			/// <summary>
			/// Gets the specified appender
			/// </summary>
			/// <param name="AppendName">Name of appender to find</param>
			/// <returns>List(IAppender) objects if found; NULL otherwise</returns>
			private static IEnumerable<IAppender> FindAppenders(string AppendName)
			{
				//Get a list of the current loggers
				ILog[] LoggerList = LogManager.GetCurrentLoggers();
				if (LoggerList.GetLength(0) < 1) return null;

				//Create a List of appenders matching the criteria for each logger
				var RetList = new List<IAppender>();
				foreach (ILog TestLogger in LoggerList)
				{
					foreach (IAppender TestAppender in TestLogger.Logger.Repository.GetAppenders())
					{
						if (TestAppender.Name == AppendName) RetList.Add(TestAppender);
					}
				}

				//Return the list of appenders, if any found
				if (RetList.Count > 0)
				{
					return RetList;
				}
				
				return null;

			}

			/// <summary>
			/// Sets the file logging level via an integer value (Overloaded)
			/// </summary>
			/// <param name="InpLevel">"InpLevel">Integer corresponding to level (1-5, 5 being most verbose</param>
			public static void SetFileLogLevel(int InpLevel)
			{
				Type LogLevelEnumType = typeof(LogLevels);

				//Verify input level is a valid log level
				if (!Enum.IsDefined(LogLevelEnumType, InpLevel))
				{
					WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, "Invalid value specified for level: " + InpLevel);
					return;
				}

				//Convert input integer into the associated enum
				var Lvl = (LogLevels)Enum.Parse(LogLevelEnumType, InpLevel.ToString(CultureInfo.InvariantCulture));

				SetFileLogLevel(Lvl);
			}

			/// <summary>
			/// Sets file logging level based on enumeration (Overloaded)
			/// </summary>
			/// <param name="InpLevel">LogLevels value defining level (Debug is most verbose)</param>
			public static void SetFileLogLevel(LogLevels InpLevel)
			{
				var LogRepo = (log4net.Repository.Hierarchy.Logger)m_FileLogger.Logger;

				switch (InpLevel)
				{
					case LogLevels.DEBUG:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["DEBUG"];
						break;
					case LogLevels.ERROR:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["ERROR"];
						break;
					case LogLevels.FATAL:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["FATAL"];
						break;
					case LogLevels.INFO:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["INFO"];
						break;
					case LogLevels.WARN:
						LogRepo.Level = LogRepo.Hierarchy.LevelMap["WARN"];
						break;
				}
			}

			/// <summary>
			/// Creates a file appender
			/// </summary>
			/// <param name="LogfileName">Log file name for the appender to use</param>
			/// <returns>A configured file appender</returns>
			private static FileAppender CreateFileAppender(string LogfileName)
			{
				var ReturnAppender = new FileAppender
				{
					Name = "FileAppender"
				};

				m_FileDate = DateTime.Now.ToString("MM-dd-yyyy");
				m_BaseFileName = LogfileName;
				ReturnAppender.File = m_BaseFileName + "_" + m_FileDate + ".txt";
				ReturnAppender.AppendToFile = true;
				var Layout = new log4net.Layout.PatternLayout
				{
					ConversionPattern = "%date{MM/dd/yyyy HH:mm:ss}, %message, %level,%newline"
				};
				Layout.ActivateOptions();
				ReturnAppender.Layout = Layout;
				ReturnAppender.ActivateOptions();

				return ReturnAppender;
			}

			/// <summary>
			/// Configures the file logger
			/// </summary>
			/// <param name="LogFileName">Base name for log file</param>
			/// <param name="LogLevel">Debug level for file logger</param>
			public static void CreateFileLogger(string LogFileName, int LogLevel)
			{
				var curLogger = (log4net.Repository.Hierarchy.Logger)m_FileLogger.Logger;
				m_FileAppender = CreateFileAppender(LogFileName);
				curLogger.AddAppender(m_FileAppender);
				SetFileLogLevel(LogLevel);
			}

			/// <summary>
			/// Configures the Db logger
			/// </summary>
			/// <param name="ConnStr">Database connection string</param>
			/// <param name="ModuleName">Module name used by logger</param>
			public static void CreateDbLogger(string ConnStr, string ModuleName)
			{
				var curLogger = (log4net.Repository.Hierarchy.Logger)m_DbLogger.Logger;
				curLogger.Level = log4net.Core.Level.Info;
				curLogger.AddAppender(CreateDbAppender(ConnStr, ModuleName));
				curLogger.AddAppender(m_FileAppender);
			}

			/// <summary>
			/// Creates a database appender
			/// </summary>
			/// <param name="ConnStr">Database connection string</param>
			/// <param name="ModuleName">Module name used by logger</param>
			/// <returns>ADONet database appender</returns>
			public static AdoNetAppender CreateDbAppender(string ConnStr, string ModuleName)
			{
				var ReturnAppender = new AdoNetAppender
				{
					BufferSize = 1,
					ConnectionType =
						"System.Data.SqlClient.SqlConnection, System.Data, Version=1.0.3300.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
					ConnectionString = ConnStr,
					CommandType = CommandType.StoredProcedure,
					CommandText = "PostLogEntry"
				};

				//Type parameter
				var TypeParam = new AdoNetAppenderParameter
				{
					ParameterName = "@type",
					DbType = DbType.String,
					Size = 50,
					Layout = CreateLayout("%level")
				};
				ReturnAppender.AddParameter(TypeParam);

				//Message parameter
				var MsgParam = new AdoNetAppenderParameter
				{
					ParameterName = "@message",
					DbType = DbType.String,
					Size = 4000,
					Layout = CreateLayout("%message")
				};
				ReturnAppender.AddParameter(MsgParam);

				//PostedBy parameter
				var PostByParam = new AdoNetAppenderParameter
				{
					ParameterName = "@postedBy",
					DbType = DbType.String,
					Size = 128,
					Layout = CreateLayout(ModuleName)
				};
				ReturnAppender.AddParameter(PostByParam);

				ReturnAppender.ActivateOptions();

				return ReturnAppender;
			}

			/// <summary>
			/// Creates a layout object for a Db appender parameter
			/// </summary>
			/// <param name="LayoutStr">Name of parameter</param>
			/// <returns></returns>
			private static log4net.Layout.IRawLayout CreateLayout(string LayoutStr)
			{
				var layoutConvert = new log4net.Layout.RawLayoutConverter();
				var returnLayout = new log4net.Layout.PatternLayout
				{
					ConversionPattern = LayoutStr
				};
				returnLayout.ActivateOptions();
				var retItem = (log4net.Layout.IRawLayout)layoutConvert.ConvertFrom(returnLayout);
				return retItem;
			}

		#endregion
	}	// End class
}	// End namespace
