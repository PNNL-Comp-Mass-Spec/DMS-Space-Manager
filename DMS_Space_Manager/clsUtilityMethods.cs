
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/14/2010
//
// Last modified 09/14/2010
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Management;

namespace Space_Manager
{
	public static class clsUtilityMethods
	{
		//*********************************************************************************************************
		// Holds static utility methods that are put here to avoid cluttering up other classes
		//**********************************************************************************************************

		#region "Constants"
		#endregion

		#region "Class variables"
		#endregion

		#region "Delegates"
		#endregion

		#region "Events"
		#endregion

		#region "Properties"
		#endregion

		#region "Constructors"
		#endregion

		#region "Methods"
			/// <summary>
			/// Parses a list of drive data objects from a string
			/// </summary>
			/// <param name="inpList">Input string containing drive information</param>
			/// <returns>List of drives with associated data</returns>
			public static List<clsDriveData> GetDriveList(string inpList)
			{
				List<clsDriveData> driveList = null;
				string[] driveArray;
				string[] driveInfo;

				// Data for drives is separated by semi-colon.
				driveArray = inpList.Split(new char[] { ';' });
				if (driveArray == null)
				{
					// There were no drives in string
					string msg = "No drives found in drive list " + inpList;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return null;
				}

				driveList = new List<clsDriveData>();

				// Data for an individual drive is separated by comma
				foreach (string drive in driveArray)
				{
					driveInfo = drive.Split(new char[] { ',' });
					if (driveInfo == null)
					{
						string msg = "Unable to get drive space from string " + driveInfo;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return null;
					}

					if (driveInfo.Length != 2)
					{
						string msg = "Invalid parameter count for drive data string " + driveInfo;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return null;
					}

					// Add the data for this drive to the return list
					clsDriveData newDrive = new clsDriveData(driveInfo[0], double.Parse(driveInfo[1]));
					driveList.Add(newDrive);
				}

				return driveList;
			}	// End sub

			/// <summary>
			/// Uses WMI to determine if free space on disk is above minimum threshold
			/// </summary>
			/// <param name="machine">Name of server to check</param>
			/// <param name="driveData">Data for drive to be checked</param>
			/// <param name="perspective">Client/Server setting for manager</param>
			/// <returns>Enum indicating space status</returns>
            public static SpaceCheckResults IsPurgeRequired(string machine, string perspective, clsDriveData driveData, out double driveFreeSpaceGB)
			{
				double availableSpace = 0;
				SpaceCheckResults testResult = SpaceCheckResults.Error;
				// Set client/server flag based on config
				bool client = perspective == "client" ? true : false;
                driveFreeSpaceGB = -1;
                
				if (client)
				{
                    // Checking a remote drive
                    // Get WMI object representing drive
                    string requestStr;
                    requestStr = @"\\" + machine + @"\root\cimv2:win32_logicaldisk.deviceid=""" + driveData.DriveLetter + "\"";

                    try
                    {
                        ManagementObject disk = new ManagementObject(requestStr);
                        disk.Get();
                        
                        object oFreeSpace = disk["FreeSpace"];
                        double totalSpace;
                        if (oFreeSpace == null)
                        {
                            string msg = "Drive " + driveData.DriveLetter + " not found via WMI; likely is Not Ready";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                            return SpaceCheckResults.Error;
                        }
                        else
                        {
                            availableSpace = System.Convert.ToDouble(oFreeSpace);
                            totalSpace = System.Convert.ToDouble(disk["Size"]);
                        }
                       
                        if (totalSpace <= 0)
                        {
                            string msg = "Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via WMI; likely is Not Ready";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                            return SpaceCheckResults.Error;
                        }
                        else
                        {
                            driveFreeSpaceGB = availableSpace / Math.Pow(2D, 30D);	// Convert to GB
                        }
                    }
                    catch (Exception ex)
                    {
                        string msg = "Exception getting free disk space using WMI, drive " + driveData.DriveLetter + ": " + ex.Message;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                        testResult = SpaceCheckResults.Error;
                        if (driveFreeSpaceGB > 0)
                            driveFreeSpaceGB = -driveFreeSpaceGB;
                        if (driveFreeSpaceGB == 0)
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
                        System.IO.DriveInfo diDrive = new System.IO.DriveInfo(driveData.DriveLetter);

                        if (!diDrive.IsReady)
                        {
                            string msg = "Drive " + driveData.DriveLetter + " reports Not Ready via DriveInfo object; drive is offline or drive letter is invalid";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                            return SpaceCheckResults.Error;
                        }
                        else
                        {
                            if (diDrive.TotalSize <= 0)
                            {
                                string msg = "Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via DriveInfo object; likely is Not Ready";
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                                return SpaceCheckResults.Error;
                            }
                            else
                            {
                                driveFreeSpaceGB = diDrive.TotalFreeSpace / Math.Pow(2D, 30D);	// Convert to GB
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        string msg = "Exception getting free disk space via .NET DriveInfo object, drive " + driveData.DriveLetter + ": " + ex.Message;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                        testResult = SpaceCheckResults.Error;
                        if (driveFreeSpaceGB > 0)
                            driveFreeSpaceGB = -driveFreeSpaceGB;
                        if (driveFreeSpaceGB == 0)
                            driveFreeSpaceGB = -1;
                    }

				}

                if (driveFreeSpaceGB < 0)
                {
                    testResult = SpaceCheckResults.Error;

                    // Log space requirement if debug logging enabled
                    string spaceMsg = "Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace.ToString() +
                        ", Drive not found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, spaceMsg);

                }
                else
                {
                    if (driveFreeSpaceGB > driveData.MinDriveSpace)
                    {
                        testResult = SpaceCheckResults.Above_Threshold;
                    }
                    else
                    {
                        testResult = SpaceCheckResults.Below_Threshold;
                    }

                    // Log space requirement if debug logging enabled
                    string spaceMsg = "Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace.ToString() +
                        ", Avail space: " + driveFreeSpaceGB.ToString("####0.0");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, spaceMsg);

                }

				return testResult;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
