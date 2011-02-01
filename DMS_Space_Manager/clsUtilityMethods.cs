
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
			public static SpaceCheckResults IsPurgeRequired(string machine, string perspective, clsDriveData driveData)
			{
				double availableSpace = 0;
				string requestStr;
				SpaceCheckResults testResult = SpaceCheckResults.Error;
				// Set client/server flag based on config
				bool client = perspective == "client" ? true : false;

				// Get WMI object representing drive
				if (client)
				{
					requestStr = @"\\" + machine + @"\root\cimv2:win32_logicaldisk.deviceid=""" + driveData.DriveLetter + "\"";
				}
				else
				{
					requestStr = "win32_logicaldisk.deviceid=\"" + driveData.DriveLetter + "\"";
				}

				try
				{
					ManagementObject disk = new ManagementObject(requestStr);
					disk.Get();
					availableSpace = System.Convert.ToDouble(disk["FreeSpace"]);
					availableSpace /= Math.Pow(2D, 30D);	// Convert to GB
					if (availableSpace > driveData.MinDriveSpace)
					{
						testResult = SpaceCheckResults.Above_Threshold;
					}
					else testResult = SpaceCheckResults.Below_Threshold;
				}
				catch (Exception ex)
				{
					string msg = "Exception getting free disk space, drive " + driveData.DriveLetter + ". Message = " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					testResult = SpaceCheckResults.Error;
				}

				// Log space requirement if debug logging enabled
				string spaceMsg = "Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace.ToString() + 
					", Avail space: " + availableSpace.ToString("####0.0");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, spaceMsg);

				return testResult;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
