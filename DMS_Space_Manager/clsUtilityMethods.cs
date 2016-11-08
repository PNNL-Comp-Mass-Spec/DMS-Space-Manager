
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/14/2010
//
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Management;

namespace Space_Manager
{
    public static class clsUtilityMethods
    {
        //*********************************************************************************************************
        // Holds static utility methods that are put here to avoid cluttering up other classes
        //**********************************************************************************************************

        #region "Methods"

        /// <summary>
        /// Convert bytes to Gigabytes
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        private static double BytesToGB(long bytes)
        {
            return bytes / 1024.0 / 1024.0 / 1024.0;
        }

        /// <summary>
        /// Parses a list of drive data objects from a string
        /// </summary>
        /// <param name="inpList">Input string containing drive information</param>
        /// <returns>List of drives with associated data</returns>
        public static IEnumerable<clsDriveData> GetDriveList(string inpList)
        {
            if (string.IsNullOrWhiteSpace(inpList))
            {
                // There were no drives in string
                LogError("Drive list provided to GetDriveList is empty");
                return null;
            }

            // Data for drives is separated by semi-colon.
            var driveArray = inpList.Split(';');

            var driveList = new List<clsDriveData>();

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
                var newDrive = new clsDriveData(driveInfo[0], double.Parse(driveInfo[1]));
                driveList.Add(newDrive);
            }

            return driveList;
        }

        /// <summary>
        /// For remote drives, uses WMI to determine if free space on disk is above minimum threshold
        /// For local drives, uses DriveInfo
        /// </summary>
        /// <param name="machine">Name of server to check</param>
        /// <param name="driveData">Data for drive to be checked</param>
        /// <param name="perspective">Client/Server setting for manager.  "Client" means checking a remote drive; "Server" means running on a Proto-x server </param>
        /// <param name="driveFreeSpaceGB">Actual drive free space in GB</param>
        /// <returns>Enum indicating space status</returns>
        public static SpaceCheckResults IsPurgeRequired(string machine, string perspective, clsDriveData driveData, out double driveFreeSpaceGB)
        {
            SpaceCheckResults testResult;

            driveFreeSpaceGB = -1;

            if (perspective.StartsWith("client", StringComparison.InvariantCultureIgnoreCase))
            {
                // Checking a remote drive
                // Get WMI object representing drive
                var requestStr = @"\\" + machine + @"\root\cimv2:win32_logicaldisk.deviceid=""" + driveData.DriveLetter + "\"";

                try
                {
                    var disk = new ManagementObject(requestStr);
                    disk.Get();

                    var oFreeSpace = disk["FreeSpace"];
                    if (oFreeSpace == null)
                    {
                        LogError("Drive " + driveData.DriveLetter + " not found via WMI; likely is Not Ready", true);
                        return SpaceCheckResults.Error;
                    }

                    var availableSpace = Convert.ToDouble(oFreeSpace);
                    var totalSpace = Convert.ToDouble(disk["Size"]);

                    if (totalSpace <= 0)
                    {
                        LogError("Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via WMI; likely is Not Ready", true);
                        return SpaceCheckResults.Error;
                    }

                    driveFreeSpaceGB = BytesToGB((long)availableSpace);
                }
                catch (Exception ex)
                {
                    var msg = "Exception getting free disk space using WMI, drive " + driveData.DriveLetter + ": " + ex.Message;

                    var postToDB = !Environment.MachineName.StartsWith("monroe", StringComparison.InvariantCultureIgnoreCase);
                    LogError(msg, postToDB);

                    if (driveFreeSpaceGB > 0)
                        driveFreeSpaceGB = -driveFreeSpaceGB;

                    if (Math.Abs(driveFreeSpaceGB) < float.Epsilon)
                        driveFreeSpaceGB = -1;
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
                ReportStatus("Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace + ", Drive not found", true);
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
        /// Log an error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="postToDatabase">When true, log the message to the database and the local log file</param>
        public static void LogError(string errorMessage, bool postToDatabase = false)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorMessage);
            Console.ResetColor();

            var loggerType = postToDatabase ? clsLogTools.LoggerTypes.LogDb : clsLogTools.LoggerTypes.LogFile;
            clsLogTools.WriteLog(loggerType, clsLogTools.LogLevels.ERROR, errorMessage);
        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ex">Exception to log</param>
        public static void LogError(string errorMessage, Exception ex)
        {
            ReportStatus(errorMessage, ex);
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        public static void LogWarning(string warningMessage)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(warningMessage);
            Console.ResetColor();
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage);
        }

        /// <summary>
        /// Shows information about an exception at the console and in the log file
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception</param>
        public static void ReportStatus(string errorMessage, Exception ex)
        {
            string formattedError;
            if (errorMessage.EndsWith(ex.Message))
            {
                formattedError = errorMessage;
            }
            else
            {
                formattedError = errorMessage + ": " + ex.Message;
            }
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(formattedError);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(PRISM.Logging.Utilities.GetExceptionStackTraceMultiLine(ex));
            Console.ResetColor();
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, formattedError, ex);
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isDebug">True if a debug level message</param>
        public static void ReportStatus(string statusMessage, bool isDebug = false)
        {
            Console.WriteLine(statusMessage);
            var logLevel = isDebug ? clsLogTools.LogLevels.DEBUG : clsLogTools.LogLevels.INFO;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, logLevel, statusMessage);
        }

        #endregion
    }
}
