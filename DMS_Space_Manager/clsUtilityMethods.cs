
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

        #region "Enums"
        public enum StoredProcedureExecutionResult
        {
            OK = 0,
            Deadlock = -4,
            Excessive_Retries = -5
        }
        #endregion

        #region "Methods"

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
                const string msg = "Drive list provided to GetDriveList is empty";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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
                    var msg = "Unable to get drive space threshold from string, should be something like G:,600 and not " + driveSpec;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    return null;
                }

                var driveInfo = driveSpec.Split(',');

                if (driveInfo.Length != 2)
                {
                    var msg = "Invalid parameter count for drive data string " + driveSpec + ", should be something like G:,600";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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
                        var msg = "Drive " + driveData.DriveLetter + " not found via WMI; likely is Not Ready";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                        return SpaceCheckResults.Error;
                    }

                    var availableSpace = Convert.ToDouble(oFreeSpace);
                    var totalSpace = Convert.ToDouble(disk["Size"]);

                    if (totalSpace <= 0)
                    {
                        var msg = "Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via WMI; likely is Not Ready";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                        return SpaceCheckResults.Error;
                    }

                    driveFreeSpaceGB = availableSpace / Math.Pow(2D, 30D);	// Convert to GB
                }
                catch (Exception ex)
                {
                    var msg = "Exception getting free disk space using WMI, drive " + driveData.DriveLetter + ": " + ex.Message;

                    if (Environment.MachineName.StartsWith("monroe", StringComparison.InvariantCultureIgnoreCase))
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    else
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

                    if (driveFreeSpaceGB > 0)
                        driveFreeSpaceGB = -driveFreeSpaceGB;

                    if (Math.Abs(driveFreeSpaceGB) < Single.Epsilon)
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
                        var msg = "Drive " + driveData.DriveLetter + " reports Not Ready via DriveInfo object; drive is offline or drive letter is invalid";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                        return SpaceCheckResults.Error;
                    }

                    if (diDrive.TotalSize <= 0)
                    {
                        var msg = "Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via DriveInfo object; likely is Not Ready";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                        return SpaceCheckResults.Error;
                    }

                    driveFreeSpaceGB = diDrive.TotalFreeSpace / Math.Pow(2D, 30D);	// Convert to GB
                }
                catch (Exception ex)
                {
                    var msg = "Exception getting free disk space via .NET DriveInfo object, drive " + driveData.DriveLetter + ": " + ex.Message;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

                    if (driveFreeSpaceGB > 0)
                        driveFreeSpaceGB = -driveFreeSpaceGB;
                    if (Math.Abs(driveFreeSpaceGB) < Single.Epsilon)
                        driveFreeSpaceGB = -1;
                }

            }

            if (driveFreeSpaceGB < 0)
            {
                testResult = SpaceCheckResults.Error;

                // Log space requirement if debug logging enabled
                var spaceMsg = "Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace + ", Drive not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, spaceMsg);
            }
            else
            {
                if (driveFreeSpaceGB > driveData.MinDriveSpace)
                    testResult = SpaceCheckResults.Above_Threshold;
                else
                    testResult = SpaceCheckResults.Below_Threshold;

                // Log space requirement if debug logging enabled
                var spaceMsg = "Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace + ", Avail space: " + driveFreeSpaceGB.ToString("####0.0");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, spaceMsg);

            }

            return testResult;
        }
        #endregion
    }
}
