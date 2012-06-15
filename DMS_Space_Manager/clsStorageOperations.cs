
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
		const string WAITING_FOR_HASH_FILE = "(HASH)";
		const string HASH_NOT_FOUND = "(HASH NOT FOUND)";
		const string HASH_MISMATCH = "#HashMismatch#";

		#endregion

		#region "Enums"
		public enum ArchiveCompareResults
		{
			Compare_Equal,
			Compare_Not_Equal,
			Compare_Storage_Server_Folder_Missing,
			Compare_Error,
			Compare_Waiting_For_Hash,
			Hash_Not_Found_For_File,
			Compare_Archive_Samba_Share_Missing
		}

		public enum PurgePolicyConstants
		{
			Auto = 0,
			PurgeAllExceptQC = 1,
			PurgeAll = 2
		}

		#endregion

		#region "Structures"
		public struct udtDatasetInfoType
		{
			public string DatasetName;
			public string DatasetFolderName;
			public string Instrument;
			public string YearQuarter;
			public string ServerFolderPath;				// Folder path of this dataset on the storage server
			public PurgePolicyConstants PurgePolicy;
			public string RawDataType;
		}
		#endregion

		#region "Class variables"
		IMgrParams m_MgrParams;
		bool m_ClientPerspective = false;

		string m_MD5ResultsFileDatasetName = string.Empty;
		string m_MD5ResultsFilePath = string.Empty;

		// This dictionary object contains the full path to a file as the key and the MD5 hash as the value
		// File paths are not case sensitive
		System.Collections.Generic.Dictionary<string, string> m_HashFileContents;

		string m_LastMD5WarnDataset = string.Empty;

		#endregion

		#region "Properties"
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
			string datasetPathSamba = "";
			string msg = "";
			bool retVal;

			udtDatasetInfoType udtDatasetInfo = new udtDatasetInfoType();

			udtDatasetInfo.DatasetName = purgeParams.GetParam("dataset");
			udtDatasetInfo.DatasetFolderName = purgeParams.GetParam("Folder");
			udtDatasetInfo.Instrument = purgeParams.GetParam("Instrument");
			udtDatasetInfo.YearQuarter = purgeParams.GetParam("DatasetYearQuarter");
			udtDatasetInfo.ServerFolderPath = string.Empty;
			udtDatasetInfo.PurgePolicy = GetPurgePolicyEnum(purgeParams.GetParam("PurgePolicy"));
			udtDatasetInfo.RawDataType = purgeParams.GetParam("RawDataType");

			// Get path to dataset folder on server
			{
				if (m_ClientPerspective)
				{
					// Manager is running on a client
					udtDatasetInfo.ServerFolderPath = purgeParams.GetParam("StorageVolExternal");
				}
				else
				{
					//Manager is running on storage server
					udtDatasetInfo.ServerFolderPath = purgeParams.GetParam("StorageVol");
				}
				udtDatasetInfo.ServerFolderPath = System.IO.Path.Combine(udtDatasetInfo.ServerFolderPath, purgeParams.GetParam("storagePath"));
				udtDatasetInfo.ServerFolderPath = System.IO.Path.Combine(udtDatasetInfo.ServerFolderPath, udtDatasetInfo.DatasetFolderName);

				//Get path to dataset folder in archive
				datasetPathSamba = Path.Combine(purgeParams.GetParam("SambaStoragePath"), udtDatasetInfo.DatasetFolderName);
			}

			msg = "Verifying integrity vs. archive, dataset " + udtDatasetInfo.ServerFolderPath;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			System.Collections.Generic.SortedSet<string> lstServerFilesToPurge;
			System.Collections.Generic.List<int> lstJobsToPurge;
			ArchiveCompareResults CompRes = CompareDatasetFolders(udtDatasetInfo, datasetPathSamba, out lstServerFilesToPurge, out lstJobsToPurge);

			switch (CompRes)
			{
				case ArchiveCompareResults.Compare_Equal:
					// Everything matches up
					break;

				case ArchiveCompareResults.Compare_Storage_Server_Folder_Missing:
					// Confirm that the share for the dataset actual exists
					if (ValidateDatasetShareExists(udtDatasetInfo.ServerFolderPath))
					{
						// Share exists; return Failed since we likely need to update the database
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
					else
					{
						// Share is missing
						return EnumCloseOutType.CLOSEOUT_DRIVE_MISSING;
					}

				case ArchiveCompareResults.Compare_Error:
					// Unable to perform comparison operation; set purge task failed
					//	Error was logged during comparison
					return EnumCloseOutType.CLOSEOUT_FAILED;

				case ArchiveCompareResults.Compare_Not_Equal:
					// Sever/Archive mismatch; an archive update is required before purging
					retVal = UpdateMD5ResultsFile(udtDatasetInfo);
					return EnumCloseOutType.CLOSEOUT_UPDATE_REQUIRED;

				case ArchiveCompareResults.Compare_Waiting_For_Hash:
					// MD5 results file not found, but stagemd5 file exists. Skip dataset and tell DMS to try again later
					retVal = UpdateMD5ResultsFile(udtDatasetInfo);
					return EnumCloseOutType.CLOSEOUT_WAITING_HASH_FILE;

				case ArchiveCompareResults.Compare_Archive_Samba_Share_Missing:
					// Archive share is missing
					return EnumCloseOutType.CLOSEOUT_DRIVE_MISSING;

				default:
					// Unrecognized result code
					return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			if ((lstServerFilesToPurge.Count == 0))
			{
				// Nothing was found to purge.
				msg = "No purgeable data found for datset " + udtDatasetInfo.DatasetName + ", purge policy = " + GetPurgePolicyDescription(udtDatasetInfo.PurgePolicy);

				switch (udtDatasetInfo.PurgePolicy)
				{
					case PurgePolicyConstants.Auto:
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						return EnumCloseOutType.CLOSEOUT_PURGE_AUTO;

					case PurgePolicyConstants.PurgeAllExceptQC:
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						return EnumCloseOutType.CLOSEOUT_PURGE_ALL_EXCEPT_QC;

					case PurgePolicyConstants.PurgeAll:
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;

					default:
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
				}

			}


#if DoDelete
			//Purge the dataset folder by deleting contents
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Purging dataset " + udtDatasetInfo.ServerFolderPath);
#else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "SIMULATE: Purging dataset " + udtDatasetInfo.ServerFolderPath);
#endif

			// This list keeps track of the folders that we are processing
			System.Collections.Generic.SortedSet<string> lstServerFolders = new System.Collections.Generic.SortedSet<string>();

			int iFilesDeleted = 0;
			int iFoldersDeleted = 0;

			// Delete the files listed in lstServerFilesToPurge
			// If the PurgePolicy is AutoPurge or Delete All Except QC then the files in lstServerFilesToPurge could be a subset of the actual files present
			foreach (string fileToDelete in lstServerFilesToPurge)
			{
				try
				{
					System.IO.FileInfo fiFile = new System.IO.FileInfo(fileToDelete);
					if (!lstServerFolders.Contains(fiFile.Directory.FullName))
						lstServerFolders.Add(fiFile.Directory.FullName);

					if (fiFile.Exists)
					{
#if DoDelete
						// This code will only be reached if conditional compilation symbol DoDelete is defined
						try
						{
							fiFile.Delete();
						}
						catch
						{
							// Check the ReadOnly flag then retry the deletion
							if ((fiFile.Attributes & System.IO.FileAttributes.ReadOnly) == System.IO.FileAttributes.ReadOnly)
								fiFile.Attributes = fiFile.Attributes & ~System.IO.FileAttributes.ReadOnly;

							fiFile.Delete();
						}
#endif
						iFilesDeleted += 1;
					}

				}
				catch (Exception ex)
				{
					msg = "Exception deleting file " + fileToDelete + "; " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}

			// Log debug message
			msg = "Deleted " + iFilesDeleted + " file" + CheckPlural(iFilesDeleted);
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Look for empty folders that can now be deleted
			foreach (string serverFolder in lstServerFolders)
			{
				try
				{
					if (serverFolder != udtDatasetInfo.ServerFolderPath)
						// Note that this function will only delete the folder if conditional compilation symbol DoDelete is defined and if the folder is empty
						DeleteFolderIfEmpty(serverFolder, ref iFoldersDeleted);
				}
				catch (Exception ex)
				{
					msg = "Exception deleting folder " + serverFolder + "; " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}

			// Log debug message
			msg = "Deleted " + iFoldersDeleted + " empty folder" + CheckPlural(iFoldersDeleted);
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Delete the dataset folder if it is empty
			try
			{
				// Note that this function will only delete the folder if conditional compilation symbol DoDelete is defined and if the folder is empty
				DeleteFolderIfEmpty(udtDatasetInfo.ServerFolderPath, ref iFoldersDeleted);
			}
			catch (Exception ex)
			{
				msg = "Exception deleting dataset folder " + udtDatasetInfo.ServerFolderPath + "; " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return EnumCloseOutType.CLOSEOUT_FAILED;
			}

			// Log debug message
			msg = "Purged files and folders from dataset folder " + udtDatasetInfo.ServerFolderPath;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Mark the jobs in lstJobsToPurge as purged
			MarkPurgedJobs(lstJobsToPurge);

			// If we got to here, then log success and exit
			msg = "Purged dataset " + udtDatasetInfo.DatasetName + ", purge policy = " + GetPurgePolicyDescription(udtDatasetInfo.PurgePolicy);

#if !DoDelete
			msg = "SIMULATE: " + msg;
#endif

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			switch (udtDatasetInfo.PurgePolicy)
			{
				case PurgePolicyConstants.Auto:
					return EnumCloseOutType.CLOSEOUT_PURGE_AUTO;

				case PurgePolicyConstants.PurgeAllExceptQC:
					return EnumCloseOutType.CLOSEOUT_PURGE_ALL_EXCEPT_QC;

				case PurgePolicyConstants.PurgeAll:
					return EnumCloseOutType.CLOSEOUT_SUCCESS;

				default:
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unrecognized purge policy");
					return EnumCloseOutType.CLOSEOUT_FAILED;
			}

		}	// End Sub

		/// <summary>
		/// Returns "s" if the value is 0 or greater than 1
		/// Returns "" if the value is 1
		/// </summary>
		/// <param name="iValue"></param>
		/// <returns></returns>
		protected string CheckPlural(int iValue)
		{
			if (iValue == 1)
				return string.Empty;
			else
				return "s";
		}

		/// <summary>
		/// Compares the contents of two dataset folders
		/// </summary>
		/// <param name="datasetName">Neme of dataset</param>
		/// <param name="sambaDatasetNamePath">Location of dataset folder in archive (samba)</param>
		/// <returns></returns>
		public ArchiveCompareResults CompareDatasetFolders(udtDatasetInfoType udtDatasetInfo, string sambaDatasetNamePath,
			out System.Collections.Generic.SortedSet<string> lstServerFilesToPurge,
			out System.Collections.Generic.List<int> lstJobsToPurge)
		{
			lstServerFilesToPurge = new System.Collections.Generic.SortedSet<string>();
			lstJobsToPurge = new System.Collections.Generic.List<int>();

			string archFilePath;
			string msg;

			string sMismatchMessage = string.Empty;
			ArchiveCompareResults eCompResultOverall = ArchiveCompareResults.Compare_Equal;
			System.IO.DirectoryInfo diDatasetFolder = new System.IO.DirectoryInfo(udtDatasetInfo.ServerFolderPath);

			// Verify server dataset folder exists. If it doesn't, then either we're getting Access Denied or the folder was manually purged
			if (!diDatasetFolder.Exists)
			{
				msg = "clsUpdateOps.CompareDatasetFolders, folder " + udtDatasetInfo.ServerFolderPath + " not found; either the folder was manually purged or Access is Denied";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
				return ArchiveCompareResults.Compare_Storage_Server_Folder_Missing;
			}

			//Verify Samba dataset folder exists
			if (!Directory.Exists(sambaDatasetNamePath))
			{
				msg = "clsUpdateOps.CompareDatasetFolders, folder " + sambaDatasetNamePath + " not found; unable to verify files prior to purge";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
				return ArchiveCompareResults.Compare_Archive_Samba_Share_Missing;
			}
			
			// If the dataset folder is empty yet the parent folder exists, then assume it was manually purged; just update the database
			if (diDatasetFolder.GetFileSystemInfos().Length == 0 && diDatasetFolder.Parent.Exists)
			{
				msg = "clsUpdateOps.CompareDatasetFolders, folder " + udtDatasetInfo.ServerFolderPath + " is empty; assuming manually purged";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
				return ArchiveCompareResults.Compare_Equal;
			}

			// Find files to purge based on the purge policy
			clsPurgeableFileSearcher oPurgeableFileSearcher = new clsPurgeableFileSearcher();
			lstServerFilesToPurge = oPurgeableFileSearcher.FindDatasetFilesToPurge(diDatasetFolder, udtDatasetInfo, out lstJobsToPurge);

			// Loop through the file list, checking for archive copies and comparing if archive copy present
			// We need to generate a hash for all of the files so that we can remove invalid lines from m_HashFileContents if a hash mis-match is present
			foreach (string sServerFilePath in lstServerFilesToPurge)
			{
				// Convert the file name on the storage server to its equivalent in the archive
				archFilePath = ConvertServerPathToArchivePath(udtDatasetInfo.ServerFolderPath, sambaDatasetNamePath, sServerFilePath);
				if (archFilePath.Length == 0)
				{
					msg = "File name not returned when converting from server path to archive path for file" + sServerFilePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return ArchiveCompareResults.Compare_Error;
				}
				else if (archFilePath == "Error")
				{
					//Error was logged by called function, so just return
					return ArchiveCompareResults.Compare_Error;
				}

				//Determine if file exists in archive
				System.IO.FileInfo fiArchiveFile = new System.IO.FileInfo(archFilePath);

				if (fiArchiveFile.Exists)
				{
					// File exists in archive, so compare the server and archive versions
					ArchiveCompareResults CompRes = CompareTwoFiles(sServerFilePath, archFilePath, udtDatasetInfo);

					if (CompRes == ArchiveCompareResults.Compare_Equal)
					{
						// Hash codes match; continue checking
					}
					else if (CompRes == ArchiveCompareResults.Compare_Not_Equal)
					{
						// An update is required
						if (string.IsNullOrEmpty(sMismatchMessage) || eCompResultOverall != ArchiveCompareResults.Compare_Not_Equal)
							sMismatchMessage = "  Update required. Server file " + sServerFilePath + " doesn't match archive file " + archFilePath;

						eCompResultOverall = ArchiveCompareResults.Compare_Not_Equal;
					}
					else if (CompRes == ArchiveCompareResults.Compare_Waiting_For_Hash || CompRes == ArchiveCompareResults.Hash_Not_Found_For_File)
					{

						// If this file is over AGED_FILE_DAYS days old and is in a subfolder then only compare file dates
						// If the file in the archive is newer than this file, then assume the archive copy is valid
						// Prior to January 2012 we would assume the files are not equal (since no hash) and would not purge this dataset
						// Due to the slow speed of restoring files to tape we have switched to simply comparing dates
						//
						// In June 2012 we changed AGED_FILE_DAYS from 240 to 45 days since the archive retention period has become quite short

						const int AGED_FILE_DAYS = 45;

						System.IO.FileInfo fiServerFile = new System.IO.FileInfo(sServerFilePath);
						bool bAssumeEqual = false;
						double dFileAgeDays = System.DateTime.UtcNow.Subtract(fiServerFile.LastWriteTimeUtc).TotalDays;

						if (dFileAgeDays >= AGED_FILE_DAYS ||
							dFileAgeDays >= 30 && diDatasetFolder.Name.ToLower().StartsWith("blank"))
						{
							if (fiServerFile.Length == fiArchiveFile.Length && fiServerFile.LastWriteTimeUtc <= fiArchiveFile.LastWriteTimeUtc)
							{
								// Copy in archive is the same size and same date (or newer)
								if (fiServerFile.DirectoryName != diDatasetFolder.FullName)
								{
									// File is in a subfolder; assume equal and continue checking
									bAssumeEqual = true;
								}
								else
								{
									if (fiServerFile.Name.StartsWith("x_") || fiServerFile.Name == "metadata.xml" || fiServerFile.Name == "metadata.txt")
									{
										// File is not critical; assume equal and continue checking
										bAssumeEqual = true;
									}
								}
							}

						}

						if (bAssumeEqual)
						{
							msg = "    ignoring aged, non-critical file: " + fiServerFile.FullName.Replace(diDatasetFolder.FullName, "").Substring(1);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						}
						else
						{

							if (CompRes == ArchiveCompareResults.Compare_Waiting_For_Hash)
							{
								// A hash file wasn't found. Skip dataset and notify DMS to try again later
								// This is logged as a debug message since we've already logged "Found stagemd5 file: \\a1.emsl.pnl.gov\dmsmd5\stagemd5.DatasetName"
								msg = "  Waiting for hash file generation";
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
								return ArchiveCompareResults.Compare_Waiting_For_Hash;
							}
							else if (CompRes == ArchiveCompareResults.Hash_Not_Found_For_File)
							{

								if (eCompResultOverall == ArchiveCompareResults.Compare_Equal)
								{
									eCompResultOverall = ArchiveCompareResults.Compare_Waiting_For_Hash;
									sMismatchMessage = "  Hash code not found for one or more files";
								}
							}
							else
							{
								// This code should never be reached
								msg = "Logic bug, CompRes = " + CompRes.ToString() + " but should be either ArchiveCompareResults.Compare_Waiting_For_Hash or ArchiveCompareResults.Hash_Not_Found_For_File";
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
								return ArchiveCompareResults.Compare_Error;
							}

						}

					}
					else
					{
						//There was a problem with the file comparison. Error message has already been logged, so just exit
						return ArchiveCompareResults.Compare_Error;
					}
				}
				else
				{
					// File doesn't exist in archive
					// Either the archive is offline or an update is required

					if (ValidateDatasetShareExists(sambaDatasetNamePath))
					{
						msg = "  Update required. Server file not found in archive: " + sServerFilePath;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
						return ArchiveCompareResults.Compare_Not_Equal;
					}
					else
					{
						msg = "  Archive not found via samba path: " + sambaDatasetNamePath;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
						return ArchiveCompareResults.Compare_Archive_Samba_Share_Missing;
					}
					
				}
			} // foreach File in lstServerFilesToPurge

			switch (eCompResultOverall)
			{
				case ArchiveCompareResults.Compare_Equal:
					// Everything matches up
					break;

				case ArchiveCompareResults.Compare_Not_Equal:
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, sMismatchMessage);
					break;

				case ArchiveCompareResults.Compare_Waiting_For_Hash:
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, sMismatchMessage);
					break;

				default:
					break;
			}

			return eCompResultOverall;

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
		private ArchiveCompareResults CompareTwoFiles(string serverFile, string archiveFile, udtDatasetInfoType udtDatasetInfo)
		{
			string msg;

			string serverFileHash = null;	//String version of serverFile hash
			string archiveFileHash = null;	//String version of archiveFile hash
			string sFilePathInDictionary = string.Empty;

			msg = "Comparing file " + serverFile + " to file " + archiveFile;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Get hash for archive file
			archiveFileHash = GetArchiveFileHash(serverFile, udtDatasetInfo, out sFilePathInDictionary);
			if (archiveFileHash == string.Empty)
			{
				//There was a problem. Description has already been logged
				return ArchiveCompareResults.Compare_Error;
			}

			if (archiveFileHash == WAITING_FOR_HASH_FILE)
			{
				// There is no hash file, but an MD5 results file or stagemd5 file exists. Skip dataset and tell DMS to wait before trying again
				return ArchiveCompareResults.Compare_Waiting_For_Hash;
			}

			if (archiveFileHash == HASH_NOT_FOUND)
			{
				// There is a hash file, but no line exists for serverFile.
				// If this is an aged, non-critical file, we'll ignore it if the file sizes are the same and the file date/time in the archive is newer than the local copy
				return ArchiveCompareResults.Hash_Not_Found_For_File;
			}

			// Get hash for server file
			serverFileHash = GenerateHashFromFile(serverFile);
			if (string.IsNullOrEmpty(serverFileHash))
			{
				//There was a problem. Description has already been logged
				return ArchiveCompareResults.Compare_Error;
			}

			// Compare the two hash values
			if (string.Equals(serverFileHash, archiveFileHash))
			{
				return ArchiveCompareResults.Compare_Equal;
			}
			else
			{
				// Update the cached hash value to #HashMismatch#
				m_HashFileContents[sFilePathInDictionary] = HASH_MISMATCH;

				return ArchiveCompareResults.Compare_Not_Equal;
				//Files not equal
			}
		}	// End sub

		/// <summary>
		/// Delete the given folder if it is empty (no files, and all subfolders are empty)
		/// </summary>
		/// <param name="serverFolder"></param>
		/// <param name="iFoldersDeleted"></param>
		private void DeleteFolderIfEmpty(string serverFolder, ref int iFoldersDeleted)
		{

			System.IO.DirectoryInfo diFolder = new System.IO.DirectoryInfo(serverFolder);

			if (diFolder.Exists)
			{
				if (diFolder.GetFiles("*.*", SearchOption.AllDirectories).Length == 0)
				{
#if DoDelete
					// This code will only be reached if conditional compilation symbol DoDelete is defined
					DeleteFolderRecurse(diFolder.FullName);
#endif
					iFoldersDeleted += 1;
				}
			}

		}

		/// <summary>
		/// Deletes a folder, including all files and subfolders
		/// Assures that the ReadOnly bit is turned off for each folder
		/// </summary>
		/// <param name="sFolderPath"></param>
		/// <returns></returns>
		private bool DeleteFolderRecurse(string sFolderPath)
		{
			System.IO.DirectoryInfo diFolder;

			diFolder = new System.IO.DirectoryInfo(sFolderPath);

			if (diFolder.Exists)
			{
				foreach (System.IO.DirectoryInfo diSubFolder in diFolder.GetDirectories())
				{
					// Check whether the folder is marked as Read-Only
					if ((diSubFolder.Attributes & System.IO.FileAttributes.ReadOnly) == System.IO.FileAttributes.ReadOnly)
						diSubFolder.Attributes = diSubFolder.Attributes & ~System.IO.FileAttributes.ReadOnly;

					DeleteFolderRecurse(diSubFolder.FullName);
				}

				try
				{
					diFolder.Delete(true);
				}
				catch
				{
					// The folder might have readonly files
					// Manually delete each file
					DeleteFilesCheckReadonly(diFolder);
					diFolder.Delete(true);
				}

				return true;
			}
			else
			{
				// Folder not found; return true anyway
				return true;
			}

		}

		/// <summary>
		/// Deletes all files in a folder, assuring that the ReadOnly bit is turned off for each file
		/// </summary>
		/// <param name="diFolder"></param>
		private void DeleteFilesCheckReadonly(System.IO.DirectoryInfo diFolder)
		{
			foreach (System.IO.FileInfo fiFile in diFolder.GetFiles("*", System.IO.SearchOption.AllDirectories))
			{
				if ((fiFile.Attributes & System.IO.FileAttributes.ReadOnly) == System.IO.FileAttributes.ReadOnly)
					fiFile.Attributes = fiFile.Attributes & ~System.IO.FileAttributes.ReadOnly;

				fiFile.Delete();
			}
		}

		private string GenerateMD5ResultsFilePath(udtDatasetInfoType udtDatasetInfo)
		{
			string hashFileFolder = m_MgrParams.GetParam("MD5ResultsFolderPath");

			// Find out if there's a results file for this dataset
			string sMD5ResultsFilePath;

			sMD5ResultsFilePath = System.IO.Path.Combine(hashFileFolder, udtDatasetInfo.Instrument);
			sMD5ResultsFilePath = System.IO.Path.Combine(sMD5ResultsFilePath, udtDatasetInfo.YearQuarter);
			sMD5ResultsFilePath = System.IO.Path.Combine(sMD5ResultsFilePath, RESULT_FILE_NAME_PREFIX + udtDatasetInfo.DatasetName);

			return sMD5ResultsFilePath;
		}

		/// <summary>
		/// Gets the hash value for a file from the results.datasetname file in the archive
		/// </summary>
		/// <param name="fileNamePath">File on storage server to find a matching archive hatch for</param>
		/// <param name="datasetName">Name of dataset being purged</param>
		/// <returns>Hash value for success; Empty string otherwise</returns>
		private string GetArchiveFileHash(string fileNamePath, udtDatasetInfoType udtDatasetInfo, out string sFilePathInDictionary)
		{
			// Archive should have a results.datasetname file for the purge candidate dataset. If present, the file
			// will have pre-calculated hash's for the files to be deleted. The manager will look for this result file,
			// and extract the file hash if found. If hash file not found, return string that tells manager to  
			//request result file creation

			string msg;
			bool bHashFileLoaded = false;

			sFilePathInDictionary = string.Empty;

			msg = "Getting archive hash for file " + fileNamePath;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			if (!string.IsNullOrEmpty(m_MD5ResultsFileDatasetName) && string.Equals(m_MD5ResultsFileDatasetName, udtDatasetInfo.DatasetName) && m_HashFileContents != null)
			{
				// Hash file has already been loaded into memory; no need to re-load it
				bHashFileLoaded = true;
			}
			else
			{
				bool bWaitingForHashFile;

				m_HashFileContents = new System.Collections.Generic.Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

				bHashFileLoaded = LoadMD5ResultsFile(udtDatasetInfo, out bWaitingForHashFile);

				if (!bHashFileLoaded)
				{
					if (bWaitingForHashFile)
						return WAITING_FOR_HASH_FILE;
					else
						// Error occurred (and has been logged)
						return string.Empty;
				}
			}


			// Search the hash file contents for a file that matches the input file
			string filePathUnix = fileNamePath.Replace(@"\", @"/");
			string MD5HashCode = string.Empty;

			string sSubfolderTofind = "/" + udtDatasetInfo.DatasetFolderName + "/";
			string sFileNameTrimmed = TrimPathAfterSubfolder(filePathUnix, sSubfolderTofind);

			if (string.IsNullOrEmpty(sFileNameTrimmed))
			{
				msg = "  Did not find " + sSubfolderTofind + " in path " + filePathUnix + " (original path " + fileNamePath + ")";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
			}
			else
			{
				if (!m_HashFileContents.TryGetValue(sFileNameTrimmed, out MD5HashCode))
				{
					msg = "  MD5 hash not found for file " + fileNamePath + " using " + sFileNameTrimmed + "; see results file " + m_MD5ResultsFilePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					return HASH_NOT_FOUND;
				}
				else
				{
					sFilePathInDictionary = string.Copy(sFileNameTrimmed);
					return MD5HashCode;
				}
			}

			return string.Empty;

		}	// End sub

		protected string GetPurgePolicyDescription(PurgePolicyConstants ePurgePolicy)
		{
			switch (ePurgePolicy)
			{
				case PurgePolicyConstants.Auto:
					return "Auto";
				case PurgePolicyConstants.PurgeAllExceptQC:
					return "Auto all except the QC folder";
				case PurgePolicyConstants.PurgeAll:
					return "Purge all files and folders";
				default:
					return "??";

			}
		}

		protected PurgePolicyConstants GetPurgePolicyEnum(string sPurgePolicy)
		{
			int iPurgePolicy;

			if (int.TryParse(sPurgePolicy, out iPurgePolicy))
			{
				switch (iPurgePolicy)
				{
					case 0:
						return PurgePolicyConstants.Auto;
					case 1:
						return PurgePolicyConstants.PurgeAllExceptQC;
					case 2:
						return PurgePolicyConstants.PurgeAll;
					default:
						return PurgePolicyConstants.Auto;

				}
			}
			else
				return PurgePolicyConstants.Auto;

		}

		/// <summary>
		/// Loads the MD5 results file for the given dataset into memory 
		/// </summary>
		/// <param name="datasetName"></param>
		/// <returns></returns>
		private bool LoadMD5ResultsFile(udtDatasetInfoType udtDatasetInfo, out bool bWaitingForHashFile)
		{

			string msg;

			// Find out if there's an MD5 results file for this dataset
			string sMD5ResultsFilePath = GenerateMD5ResultsFilePath(udtDatasetInfo);

			bWaitingForHashFile = false;
			m_MD5ResultsFileDatasetName = string.Empty;

			try
			{
				if (!File.Exists(sMD5ResultsFilePath))
				{
					// MD5 results file not found
					if (string.Compare(udtDatasetInfo.DatasetName, m_LastMD5WarnDataset) != 0)
					{
						// Warning not yet posted
						m_LastMD5WarnDataset = String.Copy(udtDatasetInfo.DatasetName);

						msg = "  MD5 results file not found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);

						// Check to see if a stagemd5 file exists for this dataset. 
						// This is for info only since this program does not create stagemd5 files (the DatasetPurgeArchiveHelper creates them)

						string hashFileFolder = m_MgrParams.GetParam("HashFileLocation");

						string stagedFileNamePath = Path.Combine(hashFileFolder, STAGED_FILE_NAME_PREFIX + udtDatasetInfo.DatasetName);
						if (File.Exists(stagedFileNamePath))
						{

							string m_LastStageMD5WarnDataset = string.Empty;


							msg = "  Found stagemd5 file: " + stagedFileNamePath;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						}
						else
						{
							// DatasetPurgeArchiveHelper needs to create a stagemd5 file for this datatset
							// Alternatively, if there are a bunch of stagemd5 files waiting to be processed,
							//   eventually we should get MD5 result files and then we should be able to purge this dataset
							msg = "  Stagemd5 file not found";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
						}

					}

					bWaitingForHashFile = true;
					return false;
				}
			}
			catch (Exception ex)
			{
				msg = "  Exception searching for MD5 results file";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				return false;
			}

			msg = "MD5 results file for dataset found. File name = " + sMD5ResultsFilePath;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Read in results file
			try
			{
				m_HashFileContents.Clear();

				string[] sContents = File.ReadAllLines(sMD5ResultsFilePath);
				char[] cSplitChars = new char[] { ' ' };

				foreach (string sInputLine in sContents)
				{

					// Extract the MD5 results value from the found line
					// Format is MD5 code, then a space, then a full path to the file
					// Example:
					// 2036b65346acd59f3dd044b6a97bf44a /archive/dmsarch/LTQ_Orb_1/2008_1/EIF_Plasma_C_18_10Jan08_Draco_07-12-24/Seq200901221155_Auto362389/EIF_Plasma_C_18_10Jan08_Draco_07-12-24_out.zip

					string[] lineParts = sInputLine.Split(cSplitChars, 2);
					if (lineParts.Length > 1)
					{

						// For the above example, we want to store:
						// "Seq200901221155_Auto362389/EIF_Plasma_C_18_10Jan08_Draco_07-12-24_out.zip" and "2036b65346acd59f3dd044b6a97bf44a"
						string sFileNamePath = lineParts[1];
						string sSubfolderTofind = "/" + udtDatasetInfo.DatasetFolderName + "/";

						string sFileNameTrimmed = TrimPathAfterSubfolder(sFileNamePath, sSubfolderTofind);

						if (String.IsNullOrEmpty(sFileNameTrimmed))
						{
							msg = "Did not find " + sSubfolderTofind + " in line " + sInputLine + " in results file " + sMD5ResultsFilePath;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
						}
						else
						{
							// MD5 results files should not have duplicate entries, but it is possible, so need to check for this							
							if (m_HashFileContents.ContainsKey(sFileNameTrimmed))
								m_HashFileContents[sFileNameTrimmed] = lineParts[0];
							else
								m_HashFileContents.Add(sFileNameTrimmed, lineParts[0]);
						}
					}
					else
					{
						// Invalid line; skip it (but continue parsing the file)
						msg = "Unable to split line " + sInputLine + " in results file " + sMD5ResultsFilePath;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
					}
				}

			}
			catch (Exception ex)
			{
				msg = "Exception reading MD5 results file " + sMD5ResultsFilePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				return false;
			}

			// If we get here, then the file has successfully been loaded
			m_MD5ResultsFileDatasetName = string.Copy(udtDatasetInfo.DatasetName);
			m_MD5ResultsFilePath = string.Copy(sMD5ResultsFilePath);

			return true;
		}

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
		/// Call DMS to change AJ_Purged to 1 for the jobs in lstJobsToPurge
		/// </summary>
		/// <param name="lstJobsToPurge"></param>
		protected void MarkPurgedJobs(System.Collections.Generic.List<int> lstJobsToPurge)
		{
			const string SP_MARK_PURGED_JOBS = "MarkPurgedJobs";

			string msg;

			if (lstJobsToPurge.Count > 0)
			{
				// Construct a comma-separated list of jobs
				string sJobs = string.Empty;

				foreach (int job in lstJobsToPurge)
				{
					if (sJobs.Length > 0)
						sJobs += "," + job.ToString();
					else
						sJobs = job.ToString();
				}

#if DoDelete
				// Called stored procedure MarkPurgedJobs

				string connStr = m_MgrParams.GetParam("ConnectionString");
				int iMaxRetryCount = 3;
				int ResCode = 0;
				string sErrorMessage = string.Empty;

				System.Data.SqlClient.SqlParameter oParam;

				//Setup for execution of the stored procedure
				System.Data.SqlClient.SqlCommand MyCmd = new System.Data.SqlClient.SqlCommand();
				{
					MyCmd.CommandType = System.Data.CommandType.StoredProcedure;
					MyCmd.CommandText = SP_MARK_PURGED_JOBS;

					oParam = MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", System.Data.SqlDbType.Int));
					oParam.Direction = System.Data.ParameterDirection.ReturnValue;

					oParam = MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@JobList", System.Data.SqlDbType.VarChar, 4000));
					oParam.Direction = System.Data.ParameterDirection.Input;
					oParam.Value = sJobs;

					oParam = MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@InfoOnly", System.Data.SqlDbType.TinyInt));
					oParam.Direction = System.Data.ParameterDirection.Input;
					oParam.Value = 0;
				}

				//Execute the SP
				ResCode = clsUtilityMethods.ExecuteSP(MyCmd, connStr, iMaxRetryCount, out sErrorMessage);
				if (ResCode == 0)
				{
					msg = "Marked job" + CheckPlural(lstJobsToPurge.Count) + " " + sJobs + " as purged";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				else
				{
					msg = "Error calling stored procedure " + SP_MARK_PURGED_JOBS + " to mark job" + CheckPlural(lstJobsToPurge.Count) + " " + sJobs + " as purged";
					if (!string.IsNullOrEmpty(sErrorMessage))
						msg += ": " + sErrorMessage;

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
#else
				msg = "SIMULATE: call to " + SP_MARK_PURGED_JOBS + " for job" + CheckPlural(lstJobsToPurge.Count) + " " + sJobs;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
#endif

			}
		}

		/// <summary>
		/// Looks for sSubfolderTofind in sFileNamePath
		/// If found, returns the text that occurs after sSubfolderTofind
		/// If not found, then returns an empty string
		/// </summary>
		/// <param name="sFileNamePath"></param>
		/// <param name="sSubfolderTofind"></param>
		/// <returns></returns>
		private string TrimPathAfterSubfolder(string sFileNamePath, string sSubfolderTofind)
		{
			int iStartIndex = 0;

			iStartIndex = sFileNamePath.IndexOf(sSubfolderTofind);

			if (iStartIndex >= 0)
			{
				if (iStartIndex + sSubfolderTofind.Length < sFileNamePath.Length)
					return sFileNamePath.Substring(iStartIndex + sSubfolderTofind.Length);
				else
					return string.Empty;
			}
			else
			{
				return string.Empty;
			}
		}

		/// <summary>
		/// Updates the archive hash file for a dataset to only retain lines where the MD5 hash value agree
		/// </summary>
		/// <param name="dsName">Dataset name</param>
		/// <returns>TRUE for success; FALSE otherwise</returns>
		private bool UpdateMD5ResultsFile(udtDatasetInfoType udtDatasetInfo)
		{
			string msg;
			string sCurrentStep = "Start";

			// Find out if there's a master MD5 results file for this dataset
			string sMD5ResultsFileMaster = GenerateMD5ResultsFilePath(udtDatasetInfo);

			if (!File.Exists(sMD5ResultsFileMaster))
			{
				// Master MD5 results file not found; nothing to do
				return true;
			}

			// Update the hash file to remove any entries with a hash of HASH_MISMATCH in m_HashFileContents
			// These are files for which the MD5 hash of the actual file doesn't match the hash stored in the master MD5 results file, 
			//   and we thus need to re-compute the hash using the file in the Archive
			// In theory, before we do this, the Archive Update manager will update the file
			try
			{
				string sInputLine;
				string sMD5HashNew;
				System.Collections.Generic.List<string> lstUpdatedMD5Info = new System.Collections.Generic.List<string>();
				bool bWriteUpdatedMD5Info = false;

				char[] cSplitChars = new char[] { ' ' };

				string sSubfolderTofind = "/" + udtDatasetInfo.DatasetFolderName + "/";

				sCurrentStep = "Read master MD5 results file";

				System.IO.StreamReader srMD5ResultsFileMaster;

				// Open the master MD5 results file and read each line
				srMD5ResultsFileMaster = new System.IO.StreamReader(new System.IO.FileStream(sMD5ResultsFileMaster, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

				while (srMD5ResultsFileMaster.Peek() > -1)
				{
					sInputLine = srMD5ResultsFileMaster.ReadLine();

					// Extract the MD5 results value from sLineIn
					// Format is MD5 code, then a space, then a full path to the file
					// Example:
					// 2036b65346acd59f3dd044b6a97bf44a /archive/dmsarch/LTQ_Orb_1/2008_1/EIF_Plasma_C_18_10Jan08_Draco_07-12-24/Seq200901221155_Auto362389/EIF_Plasma_C_18_10Jan08_Draco_07-12-24_out.zip

					string[] lineParts = sInputLine.Split(cSplitChars, 2);
					if (lineParts.Length > 1)
					{
						// Look for the unix file path in m_HashFileContents

						string sFileNameTrimmed = TrimPathAfterSubfolder(lineParts[1], sSubfolderTofind);

						if (string.IsNullOrEmpty(sFileNameTrimmed))
						{
							// Did not find in lineParts[1]; this is unexpected
							// An error should have already been logged when function LoadMD5ResultsFile() parsed this file
						}
						else
						{

							if (m_HashFileContents.TryGetValue(sFileNameTrimmed, out sMD5HashNew))
							{
								// Match found; examine sMD5HashNew	
								if (sMD5HashNew == HASH_MISMATCH)
								{
									// We need the DatasetPurgeArchiveHelper to create a new stagemd5 file that computes a new hash for this file
									// Do not include this line in lstUpdatedMD5Info;
									bWriteUpdatedMD5Info = true;
								}
								else
								{
									// Hash codes match; retain this line
									lstUpdatedMD5Info.Add(sInputLine);
								}
							}
							else
							{
								// Match not found in m_HashFileContents
								// Retain this line
								lstUpdatedMD5Info.Add(sInputLine);
							}
						}

					}
				} // while Peek() > -1

				srMD5ResultsFileMaster.Close();

				if (bWriteUpdatedMD5Info)
				{
					string sMD5ResultsFilePathTemp = sMD5ResultsFileMaster + ".updated";
					System.IO.StreamWriter swUpdatedMD5Results;

					sCurrentStep = "Create " + sMD5ResultsFilePathTemp;
					swUpdatedMD5Results = new System.IO.StreamWriter(new System.IO.FileStream(sMD5ResultsFilePathTemp, FileMode.Create, FileAccess.Write, FileShare.Read));

					foreach (string sOutputLine in lstUpdatedMD5Info)
					{
						swUpdatedMD5Results.WriteLine(sOutputLine);
					}

					swUpdatedMD5Results.Close();
					System.Threading.Thread.Sleep(100);

					sCurrentStep = "Overwrite master MD5 results file with " + sMD5ResultsFilePathTemp;
					System.IO.File.Copy(sMD5ResultsFilePathTemp, sMD5ResultsFileMaster, true);
					System.Threading.Thread.Sleep(100);

					sCurrentStep = "Delete " + sMD5ResultsFilePathTemp;
					System.IO.File.Delete(sMD5ResultsFilePathTemp);

					msg = "  Updated MD5 results file " + sMD5ResultsFileMaster;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

				}
				else
				{
					msg = "MD5 results file does not require updating: " + sMD5ResultsFileMaster;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}

				return true;
			}
			catch (Exception ex)
			{
				msg = "Exception updating MD5 results file " + sMD5ResultsFileMaster;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				return false;
			}
		}	// End sub

		/// <summary>
		/// Validate that the share for the dataset actually exists
		/// </summary>
		/// <param name="sDatasetFolderPath"></param>
		/// <returns></returns>
		protected bool ValidateDatasetShareExists(string sDatasetFolderPath)
		{
			System.IO.DirectoryInfo diDatasetFolder;

			try
			{
				diDatasetFolder = new System.IO.DirectoryInfo(sDatasetFolderPath);

				if (diDatasetFolder.Exists)
					return true;
				else
				{
					while (diDatasetFolder.Parent != null)
					{
						diDatasetFolder = diDatasetFolder.Parent;
						if (diDatasetFolder.Exists)
							return true;
					}

					return false;
				}

			}
			catch (Exception ex)
			{
				string msg = "Exception validating that folder " + sDatasetFolderPath + " exists: " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}
		}

		#endregion
	}	// End class
}	// End namespace
