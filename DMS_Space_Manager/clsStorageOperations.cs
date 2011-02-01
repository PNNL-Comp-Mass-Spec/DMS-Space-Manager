
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/15/2010
//
// Last modified 09/15/2010
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using PRISM.Files;

namespace Space_Manager
{
	public class clsStorageOperations
	{
		//*********************************************************************************************************
		// Class to perform a purge task and all associated operations
		//**********************************************************************************************************
		#region "Constants"
			const string RESULT_FILE_NAME_PREFIX = "results.";
			const string STAGED_FILE_NAME_PREFIX = "stagemd5.";
		#endregion

		#region "Enums"
		public enum ArchiveCompareResults
			{
				Compare_Equal,
				Compare_Not_Equal,
				Compare_Storage_Server_Folder_Missing,
				Compare_Error
			}
		#endregion

		#region "Class variables"
			IMgrParams m_MgrParams;
			string m_Msg;
			bool m_ClientPerspective = false;
		#endregion

		#region "Properties"
			public string Message
			{
				get { return m_Msg; }
			}
		#endregion

		#region "Constructors"
			public clsStorageOperations(IMgrParams mgrParams)
			{
				m_MgrParams = mgrParams;

				m_ClientPerspective = m_MgrParams.GetParam("perspective") == "client" ? true : false;
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Deletes the contents of a dataset folder
			/// </summary>
			/// <param name="purgeParams">Parameters for purge operation</param>
			/// <returns>Enum representing state of task</returns>
			public EnumCloseOutType PurgeDataset(ITaskParams purgeParams)
			{
				string datasetPathSvr = "";
				string datasetPathSamba = "";
				string msg = "";
				string datasetName = purgeParams.GetParam("dataset");

				//Get path to dataset folder on server
				{
					if (m_ClientPerspective)
					{
						//Manager is running on a client
						datasetPathSvr = purgeParams.GetParam("StorageVolExternal");
					}
					else
					{
						//Manager is running on storage server
						datasetPathSvr = purgeParams.GetParam("StorageVol");
					}
					datasetPathSvr += (purgeParams.GetParam("storagePath") + purgeParams.GetParam("Folder"));

					//Get path to dataset folder in archive
					datasetPathSamba = Path.Combine(purgeParams.GetParam("SambaStoragePath"),purgeParams.GetParam("Folder"));
				}

				msg = "Verifying archive integrity, dataset " + datasetPathSvr;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				ArchiveCompareResults CompRes = CompareDatasetFolders(datasetName, datasetPathSvr, datasetPathSamba);
				switch (CompRes)
				{
					case ArchiveCompareResults.Compare_Equal:
						// Nothing needs to be done; continue with method
						break;
					case ArchiveCompareResults.Compare_Storage_Server_Folder_Missing:
						// This is OK, but we need to update the database to show dataset is purged
						return EnumCloseOutType.CLOSEOUT_SUCCESS;
					case ArchiveCompareResults.Compare_Error:
						// Unable to perform comparison operation; set purge task failed
						//	Error was logged during comparison
						return EnumCloseOutType.CLOSEOUT_FAILED;
					case ArchiveCompareResults.Compare_Not_Equal:
						// Sever/Archive mismatch; an archive update is required before purging
						bool retVal = DeleteArchiveHashResultsFile(datasetName);
						return EnumCloseOutType.CLOSEOUT_UPDATE_REQUIRED;
				}

				//Purge the dataset folder by deleting contents
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Purging dataset " + datasetPathSvr);

				//Get a file listing for the dataset folder on the server
				string[] datasetFiles = Directory.GetFiles(datasetPathSvr);
				msg = "Dataset " + datasetName + ": " + datasetFiles.GetLength(0).ToString() + " files found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				//Get a folder listing for the dataset folder on the server
				string[] datasetFolders = Directory.GetDirectories(datasetPathSvr);
				msg = "Dataset " + datasetName + ": " + datasetFolders.GetLength(0).ToString() + " folders found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				//Verify at least 1 file or folder was found to purge
				if ((datasetFiles.GetLength(0) == 0) & (datasetFolders.GetLength(0) == 0))
				{
					//Nothing was found to purge. Something's rotten in DMS
					msg = "No purgeable data found for datset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Delete the files in the dataset folder
				foreach (string fileToDelete in datasetFiles)
				{
					try
					{
						// Conditional compilation symbol DoDelete makes this true and allows deletion if defined. Otherwise,
						// condition is false and deletion will not occur

#if DoDelete
						//TODO: Find C# equivalent to VB's SetAttr: SetAttr(fileToDelete, FileAttribute.Normal)
						System.IO.File.Delete(fileToDelete);
#endif
					}
					catch (Exception ex)
					{
						msg = "Exception deleting file " + fileToDelete + "; " + ex.Message;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}

				// Log debug message
				msg = "Deleted files in dataset folder " + datasetPathSvr;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Delete the folders in the dataset folder, leaving the dataset folder intact
				foreach (string folderToDelete in datasetFolders)
				{
					try
					{
						// Conditional compilation symbol DoDelete makes this true and allows deletion if defined. Otherwise,
						// condition is false and deletion will not occur
#if DoDelete
						Directory.Delete(folderToDelete, true);
#endif
					}
					catch (Exception ex)
					{
						msg = "Exception deleting folder " + folderToDelete + "; " + ex.Message;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}

				// Log debug message
				msg = "Deleted folders in dataset folder " + datasetPathSvr;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Delete the dataset folder
				try
				{
					// Conditional compilation symbol DoDelete makes this true and allows deletion if defined. Otherwise,
					// condition is false and deletion will not occur
#if DoDelete
					Directory.Delete(datasetPathSvr, true);
#endif
				}
				catch (Exception ex)
				{
					msg = "Exception deleting dataset folder " + datasetPathSvr + "; " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Log debug message
				msg = "Deleted folders in dataset folder " + datasetPathSvr;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Delete the results.* hash file in the archive
				DeleteArchiveHashResultsFile(datasetName);

				// If we got to here, then log success and exit
				msg = "Purged dataset " + datasetName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);
				return EnumCloseOutType.CLOSEOUT_SUCCESS;
			}	// End sub

			/// <summary>
			/// Compares the contents of two dataset folders
			/// </summary>
			/// <param name="datasetName">Neme of dataset</param>
			/// <param name="svrDatasetNamePath">Location of dataset folder on server</param>
			/// <param name="sambaDatasetNamePath">Location of dataset folder in archive (samba)</param>
			/// <returns></returns>
			public ArchiveCompareResults CompareDatasetFolders(string datasetName, string svrDatasetNamePath, string sambaDatasetNamePath)
			{
				System.Collections.ArrayList serverFiles = null;
				string archFileName;
				string msg;

				//Verify server dataset folder exists. If it doesn't, that's OK - it may have been removed during a manual
				//	purge. We just need to update the database.
				if (!Directory.Exists(svrDatasetNamePath))
				{
					msg = "clsUpdateOps.CompareDatasetFolders, folder " + svrDatasetNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
					return ArchiveCompareResults.Compare_Equal;
				}

				//Verify Samba dataset folder exists
				if (!Directory.Exists(sambaDatasetNamePath))
				{
					msg = "clsUpdateOps.CompareDatasetFolders, folder " + sambaDatasetNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return ArchiveCompareResults.Compare_Error;
				}

				//Get a list of all files in the results folder and subfolders
				try
				{
					string[] DirsToScan = { svrDatasetNamePath };
					DirectoryScanner DirScanner = new DirectoryScanner(DirsToScan);
					DirScanner.PerformScan(ref serverFiles, "*.*");
				}
				catch (Exception ex)
				{
					msg = "clsUpdateOps.CompareJobFolders; exception getting file listing: " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return ArchiveCompareResults.Compare_Error;
				}

				//Loop through results folder file list, checking for archive copies and comparing if archive copy present
				foreach (string SvrFileName in serverFiles)
				{
					//Convert the file name on the storage server to its equivalent in the archive
					archFileName = ConvertServerPathToArchivePath(svrDatasetNamePath, sambaDatasetNamePath, SvrFileName);
					if (archFileName.Length == 0)
					{
						msg = "File name not returned when converting from server path to archive path for file" + SvrFileName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return ArchiveCompareResults.Compare_Error;
					}
					else if (archFileName == "Error")
					{
						//Error was logged by called function, so just return
						return ArchiveCompareResults.Compare_Error;
					}

					//Determine if file exists in archive
					if (File.Exists(archFileName))
					{
						//File exists in archive, so compare the server and archive versions
						ArchiveCompareResults CompRes = CompareTwoFiles(SvrFileName, archFileName, datasetName);
						if (CompRes == ArchiveCompareResults.Compare_Equal)
						{
							//Do nothing
						}
						else if (CompRes == ArchiveCompareResults.Compare_Not_Equal)
						{
							//An update is required
							msg = "Update required. Server file " + SvrFileName + " doesn't match archive file " + archFileName;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
							return ArchiveCompareResults.Compare_Not_Equal;
						}
						else
						{
							//There was a problem with the file comparison. Error message has already been logged, so just exit
							return ArchiveCompareResults.Compare_Error;
						}
					}
					else
					{
						//File doesn't exist in archive, so update will be required
						msg = "Update required. No copy of server file " + SvrFileName + " found in archive";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
						return ArchiveCompareResults.Compare_Not_Equal;
					}
				}

				//If we got to this point, the folders are identical
				return ArchiveCompareResults.Compare_Equal;
			}	// End sub

			/// <summary>
			/// Converts the dataet path on the server to a path in the archive
			/// </summary>
			/// <param name="datasetPathSvr">Dataset path on server</param>
			/// <param name="datasetPathArch">Dataset path on archive</param>
			/// <param name="inpFileName">Name of the file whose path is being converted</param>
			/// <returns>Full archive path to file</returns>
			private string ConvertServerPathToArchivePath(string datasetPathSvr, string datasetPathArch, string inpFileName)
			{
				string msg;

				// Convert by replacing storage server path with archive path
				try
				{
					return inpFileName.Replace(datasetPathSvr, datasetPathArch);
				}
				catch (Exception ex)
				{
					msg = "Exception converting server path to archive path for file " + datasetPathSvr + ": " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return "Error";
				}
			}	// End sub

			/// <summary>
			/// Compares two files via SHA hash
			/// </summary>
			/// <param name="inpFile1">First file to compare</param>
			/// <param name="inpFile2">Second file to compare</param>
			/// <returns>Enum containing compare results</returns>
			private ArchiveCompareResults CompareTwoFiles(string serverFile, string archiveFile, string datasetName)
			{
				string msg;

				string serverFileHash = null;	//String version of serverFile hash
				string archiveFileHash = null;	//String version of archiveFile hash

				msg = "Comparing file " + serverFile + " to file " + archiveFile;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Get hash for archive file
				archiveFileHash = GetArchiveFileHash(serverFile,datasetName);
				if (archiveFileHash == "")
				{
					//There was a problem. Description has already been logged
					return ArchiveCompareResults.Compare_Error;
				}

				//Get hash for server file
				serverFileHash = GenerateHashFromFile(serverFile);
				if (string.IsNullOrEmpty(serverFileHash))
				{
					//There was a problem. Description has already been logged
					return ArchiveCompareResults.Compare_Error;
				}

				// Compare the two hash values
				if (serverFileHash == archiveFileHash)
				{
					return ArchiveCompareResults.Compare_Equal;
				}
				else
				{
					return ArchiveCompareResults.Compare_Not_Equal;
					//Files not equal
				}
			}	// End sub

			/// <summary>
			/// Gets the hash value for a file from the results.datasetname file in the archive
			/// </summary>
			/// <param name="fileNamePath">File on storage server to find a matching archive hatch for</param>
			/// <param name="datasetName">Name of dataset being purged</param>
			/// <returns>Hash value for success; Empty string otherwise</returns>
			private string GetArchiveFileHash(string fileNamePath, string datasetName)
			{
				// Archive should have a results.datasetname file for the purge candidate dataset. If present, the file
				// will have pre-calculated hash's for the files to be deleted. The manager will look for this result file,
				// and extract the file hash if found. If not found, the purge task fails.

				string msg;
				string hashFileFolder = m_MgrParams.GetParam("HashFileLocation");

				msg = "Getting archive hash for file " + fileNamePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Find out if there's a results file for this dataset
				string hashFileNamePath = Path.Combine(hashFileFolder, RESULT_FILE_NAME_PREFIX + datasetName);

				if (!File.Exists(hashFileNamePath))
				{
					// Hash file not found in archive
					msg = "Hash results file not found for dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					// Check to see if a stagemd5 file exists for this dataset. At present, this is for info only
					string stagedFileNamePath = Path.Combine(hashFileFolder, STAGED_FILE_NAME_PREFIX + datasetName);
					if (File.Exists(stagedFileNamePath))
					{
						msg = "Staged file " + stagedFileNamePath + " exists.";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					}
					else
					{
						msg = "No stagedmd5 file found for this dataset";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					}
					return "";
				}

				msg = "Hash file for dataset found. File name = " + hashFileNamePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				string[] resultsFileLines;
				// Read in results file
				try
				{
					resultsFileLines = File.ReadAllLines(hashFileNamePath);
				}
				catch (Exception ex)
				{
					msg = "Exception reading hash file " + hashFileNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					return "";
				}

				// From the input file name, strip off all characters up to the dataset name
				string filePath;
				int dsNameStart = fileNamePath.IndexOf(datasetName);
				if (!(dsNameStart == -1))
				{
					filePath = fileNamePath.Remove(0, dsNameStart);
				}
				else
				{
					msg = "Dataset name not found in server file path " + fileNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return "";
				}

				// Search the hash file contents for a file that matches the input file
				string hashline = "";
				foreach (string testLine in resultsFileLines)
				{
					// Replace the Linux folder separation character with the Windows one
					string tmpLine = testLine.Replace(@"/", @"\");
					if (tmpLine.Contains(filePath))
					{
						hashline = tmpLine;
						break;
					}
				}

				// Was a line containing a matching file name found?
				if (hashline == "")
				{
					msg = "Hash not found for file " + fileNamePath + " in results file " + hashFileNamePath; ;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return "";
				}

				// Extract the hash falue from the found line
				string[] lineParts = hashline.Split(new char[] { ' ' });
				if (lineParts.Length > 1)
				{
					// The hash value is in the first field of the lineParts array
					return lineParts[0];
				}
				else
				{
					msg = "Invalid line " + hashline + " in results file " + hashFileNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return "";
				}
			}	// End sub

			/// <summary>
			/// Generates MD5 hash of a file's contents
			/// </summary>
			/// <param name="InpFileNamePath">Full path to file</param>
			/// <returns>String representation of hash</returns>
			private string GenerateHashFromFile(string inpFileNamePath)
			{
				string msg = null;
				byte[] byteHash = null;

				msg = "Generating hash for file " + inpFileNamePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				MD5 hashTool = MD5.Create();

				//Verify input file exists
				if (!File.Exists(inpFileNamePath))
				{
					msg = "clsUpdateOps.GenerateHashFromFile; File not found: " + inpFileNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return "";
				}

				FileInfo fi = new FileInfo(inpFileNamePath);
				Stream fStream = null;

				try
				{
					//Open the file as a stream for input to the hash class
					fStream = fi.OpenRead();
					//Get the file's hash
					byteHash = hashTool.ComputeHash(fStream);
				}
				catch (Exception ex)
				{
					msg = "clsUpdateOps.GenerateHashFromFile; Exception generating hash for file " + inpFileNamePath + ": " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return "";
				}
				finally
				{
					if ((fStream != null))
					{
						fStream.Close();
					}
				}

				// Convert hash array to hex string
				StringBuilder hashStrBld = new StringBuilder();
				for (int i = 0; i < byteHash.Length; i++)
				{
					hashStrBld.Append(byteHash[i].ToString("x2"));
				}

				return hashStrBld.ToString();
			}	// End sub

			/// <summary>
			/// Deletes the archive hash file for a dataset
			/// </summary>
			/// <param name="dsName">Dataset name</param>
			/// <returns>TRUE for success; FALSE otherwise</returns>
			private bool DeleteArchiveHashResultsFile(string dsName)
			{
				string msg;
				string hashFileFolder = m_MgrParams.GetParam("HashFileLocation");

				// Find out if there's a results file for this dataset
				string hashFileNamePath = Path.Combine(hashFileFolder, RESULT_FILE_NAME_PREFIX + dsName);
				if (!File.Exists(hashFileNamePath))
				{
					// Hash file not found in archive
					msg = "Hash results file not found for dataset " + dsName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return false;
				}

				// Delete the hash file
				try
				{
					// Conditional compilation symbol DoDelete makes this true and allows deletion if defined. Otherwise,
					// condition is false and deletion will not occur
#if DoDelete
					File.Delete(hashFileNamePath);
#endif
					msg = "Deleted archive hash file " + hashFileNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					return true;
				}
				catch (Exception ex)
				{
					msg = "Exception deleting hash file " + hashFileNamePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg,ex);
					return false;
				}
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
