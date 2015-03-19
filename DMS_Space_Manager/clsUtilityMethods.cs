
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
		/// Method for executing a db stored procedure when a data table is not returned
		/// </summary>
		/// <param name="SpCmd">SQL command object containing stored procedure params</param>
		/// <param name="ConnStr">Db connection string</param>
		/// <param name="MaxRetryCount">Maximum number of times to attempt to call the stored procedure</param>
		/// <param name="sErrorMessage">Error message (output)</param>
		/// <returns>Result code returned by SP; -1 if unable to execute SP</returns>
		/// <remarks>No logging is performed by this procedure</remarks>
		public static int ExecuteSP(System.Data.SqlClient.SqlCommand SpCmd, string ConnStr, int MaxRetryCount, out string sErrorMessage)
		{
			const int TimeoutSeconds = 30;
			return ExecuteSP(SpCmd, ConnStr, MaxRetryCount, TimeoutSeconds, out sErrorMessage);
		}

		/// <summary>
		/// Method for executing a db stored procedure when a data table is not returned
		/// </summary>
		/// <param name="SpCmd">SQL command object containing stored procedure params</param>
		/// <param name="ConnStr">Db connection string</param>
		/// <param name="MaxRetryCount">Maximum number of times to attempt to call the stored procedure</param>
		/// <param name="TimeoutSeconds">Database timeout length (seconds)</param>
		/// <param name="sErrorMessage">Error message (output)</param>
		/// <returns>Result code returned by SP; -1 if unable to execute SP</returns>
		/// <remarks>No logging is performed by this procedure</remarks>
		public static int ExecuteSP(System.Data.SqlClient.SqlCommand SpCmd, string ConnStr, int MaxRetryCount, int TimeoutSeconds, out string sErrorMessage)
		{
			//If this value is in error msg, then exception occurred before ResCode was set			
			int ResCode = -9999;			
			int RetryCount = MaxRetryCount;
			bool blnDeadlockOccurred = false;

			sErrorMessage = string.Empty;
			if (RetryCount < 1)
			{
				RetryCount = 1;
			}

			if (TimeoutSeconds == 0)
				TimeoutSeconds = 30;
			if (TimeoutSeconds < 10)
				TimeoutSeconds = 10;

			//Multiple retry loop for handling SP execution failures
			while (RetryCount > 0)
			{
				blnDeadlockOccurred = false;
				try
				{
					using (var Cn = new System.Data.SqlClient.SqlConnection(ConnStr))
					{

						Cn.Open();

						SpCmd.Connection = Cn;
						SpCmd.CommandTimeout = TimeoutSeconds;
						SpCmd.ExecuteNonQuery();

						ResCode = Convert.ToInt32(SpCmd.Parameters["@Return"].Value);

					}

					sErrorMessage = string.Empty;

					break;
				}
				catch (Exception ex)
				{
					RetryCount -= 1;
					sErrorMessage = "clsGlobal.ExecuteSP(), exception calling stored procedure " + SpCmd.CommandText + ", " + ex.Message;
					sErrorMessage += ". ResCode = " + ResCode + ". Retry count = " + RetryCount;
					Console.WriteLine(sErrorMessage);
					if (ex.Message.StartsWith("Could not find stored procedure " + SpCmd.CommandText))
					{
						// Exit out of the while loop
						break;
					}
					
					if (ex.Message.Contains("was deadlocked"))
					{
						blnDeadlockOccurred = true;
					}
				}

				if (RetryCount > 0)
				{
					//Wait 20 seconds before retrying
					System.Threading.Thread.Sleep(20000);
				}
			}

			if (RetryCount < 1)
			{
				//Too many retries, log and return error
				sErrorMessage = "Excessive retries";
				if (blnDeadlockOccurred)
				{
					sErrorMessage += " (including deadlock)";
				}
				sErrorMessage += " executing SP " + SpCmd.CommandText;
				
				Console.WriteLine(sErrorMessage);
				if (blnDeadlockOccurred)
				{
					return (int)StoredProcedureExecutionResult.Deadlock;
				}
				
				return (int)StoredProcedureExecutionResult.Excessive_Retries;
			}

			return ResCode;

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
					const string msg = "Drive list provided to GetDriveList is empty";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return null;
				}

				// Data for drives is separated by semi-colon.
				string[] driveArray = inpList.Split(new[] { ';' });

				var driveList = new List<clsDriveData>();

				// Data for an individual drive is separated by comma
                foreach (string driveSpec in driveArray)
				{
                    if (string.IsNullOrWhiteSpace(driveSpec))
					{
                        string msg = "Unable to get drive space threshold from string, should be something like G:,600 and not " + driveSpec;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return null;
					}

                    string[] driveInfo = driveSpec.Split(new[] { ',' });
					
					if (driveInfo.Length != 2)
					{
                        string msg = "Invalid parameter count for drive data string " + driveSpec + ", should be something like G:,600";
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
				var testResult = SpaceCheckResults.Error;

				driveFreeSpaceGB = -1;
                
				if (perspective.ToLower().Trim() == "client")
				{
					// Checking a remote drive
                    // Get WMI object representing drive
					string requestStr = @"\\" + machine + @"\root\cimv2:win32_logicaldisk.deviceid=""" + driveData.DriveLetter + "\"";

					try
                    {
                        var disk = new ManagementObject(requestStr);
                        disk.Get();
                        
                        object oFreeSpace = disk["FreeSpace"];
	                    if (oFreeSpace == null)
                        {
                            string msg = "Drive " + driveData.DriveLetter + " not found via WMI; likely is Not Ready";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                            return SpaceCheckResults.Error;
                        }
	                    
						double availableSpace = Convert.ToDouble(oFreeSpace);
	                    double totalSpace = Convert.ToDouble(disk["Size"]);

	                    if (totalSpace <= 0)
                        {
                            string msg = "Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via WMI; likely is Not Ready";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                            return SpaceCheckResults.Error;
                        }
	                    
						driveFreeSpaceGB = availableSpace / Math.Pow(2D, 30D);	// Convert to GB
                    }
                    catch (Exception ex)
                    {
                        string msg = "Exception getting free disk space using WMI, drive " + driveData.DriveLetter + ": " + ex.Message;
                        
						if (Environment.MachineName.ToLower().StartsWith("monroe"))
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

                        testResult = SpaceCheckResults.Error;
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
                            string msg = "Drive " + driveData.DriveLetter + " reports Not Ready via DriveInfo object; drive is offline or drive letter is invalid";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                            return SpaceCheckResults.Error;
                        }
	                    
						if (diDrive.TotalSize <= 0)
	                    {
		                    string msg = "Drive " + driveData.DriveLetter + " reports a total size of 0 bytes via DriveInfo object; likely is Not Ready";
		                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
		                    return SpaceCheckResults.Error;
	                    }
	                    
						driveFreeSpaceGB = diDrive.TotalFreeSpace / Math.Pow(2D, 30D);	// Convert to GB
                    }
                    catch (Exception ex)
                    {
                        string msg = "Exception getting free disk space via .NET DriveInfo object, drive " + driveData.DriveLetter + ": " + ex.Message;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                        testResult = SpaceCheckResults.Error;
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
                    string spaceMsg = "Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace + ", Drive not found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, spaceMsg);
                }
                else
                {
                    if (driveFreeSpaceGB > driveData.MinDriveSpace)
                        testResult = SpaceCheckResults.Above_Threshold;
                    else
                        testResult = SpaceCheckResults.Below_Threshold;

                    // Log space requirement if debug logging enabled
                    string spaceMsg = "Drive " + driveData.DriveLetter + " Space Threshold: " + driveData.MinDriveSpace + ", Avail space: " + driveFreeSpaceGB.ToString("####0.0");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, spaceMsg);

                }

				return testResult;
			}
		#endregion
	}	// End class
}	// End namespace
