using PRISM.Logging;
using PRISM;
using System;

namespace Space_Manager
{
    public abstract class LoggerBase
    {
        /// <summary>
        /// Log a debug message
        /// </summary>
        /// <param name="statusMessage"></param>
        /// <param name="writeToLog"></param>
        protected static void LogDebug(string statusMessage, bool writeToLog = true)
        {
            ReportDebug(statusMessage, writeToLog);
        }

        /// <summary>
        /// Log an error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected static void LogError(string errorMessage, bool logToDb = false)
        {
            LogTools.LogError(errorMessage, null, logToDb);
        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ex">Exception to log</param>
        protected static void LogError(string errorMessage, Exception ex)
        {
            ReportStatus(errorMessage, ex);
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected static void LogWarning(string warningMessage, bool logToDb = false)
        {
            LogTools.LogWarning(warningMessage, logToDb);
        }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Shows information about an exception at the console and in the log file
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception</param>
        protected static void ReportStatus(string errorMessage, Exception ex)
        {
            LogTools.LogError(errorMessage, ex);
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isDebug">True if a debug level message</param>
        protected static void ReportStatus(string statusMessage, bool isDebug = false)
        {
            if (isDebug)
            {
                ReportDebug(statusMessage, true);
                return;
            }

            LogTools.LogMessage(statusMessage);
        }

        /// <summary>
        /// Show a debug message, and optionally log to disk
        /// </summary>
        /// <param name="message"></param>
        /// <param name="writeToLog"></param>
        protected static void ReportDebug(string message, bool writeToLog = false)
        {
            if (writeToLog)
                LogTools.LogDebug(message);
            else
                ConsoleMsgUtils.ShowDebug(message);
        }
    }
}
