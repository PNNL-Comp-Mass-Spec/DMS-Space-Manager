//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/15/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using PRISM.AppSettings;
using PRISMDatabaseUtils;

namespace Space_Manager
{
    /// <summary>
    /// Class to perform a purge task and all associated operations
    /// </summary>
    public class StorageOperations : LoggerBase
    {
        // Ignore Spelling: dmsarch, frodo, myemsl, pre, purgeable, svc-dms

        private const string RESULT_FILE_NAME_PREFIX = "results.";
        private const string WAITING_FOR_HASH_FILE = "(HASH)";
        private const string HASH_NOT_FOUND = "(HASH NOT FOUND)";
        private const string HASH_MISMATCH = "#HashMismatch#";

        private const string PATH_CONVERSION_ERROR = "Error";

        public enum ArchiveCompareResults
        {
            /// <summary>
            /// File matches the archive or MyEMSL copy
            /// </summary>
            Compare_Equal,

            /// <summary>
            /// File does not match, or the file does not exist in the archive or MyEMSL
            /// </summary>
            Compare_Not_Equal_or_Missing,

            /// <summary>
            /// The storage server directory was not found
            /// </summary>
            Compare_Storage_Server_Directory_Missing,

            /// <summary>
            /// Comparison error
            /// </summary>
            Compare_Error,

            // Obsolete: Compare_Waiting_For_Hash,

            /// <summary>
            /// The MD5 results file does not have a hash for the file being considered for purge
            /// </summary>
            Hash_Not_Found_For_File,

            /// <summary>
            /// Directory not found: \\agate.emsl.pnl.gov\dmsarch
            /// </summary>
            /// <remarks>
            /// Previous SAMBA bridge hosts:
            ///  aurora.emsl.pnl.gov
            ///  a2.emsl.pnl.gov
            ///  adms.emsl.pnl.gov
            /// </remarks>
            Compare_Archive_Samba_Share_Missing,

            /// <summary>
            /// \\agate.emsl.pnl.gov\dmsarch exists but the dataset does not have a directory in the archive
            /// </summary>
            /// <remarks>This will be true for all datasets added to DMS after September 2013</remarks>
            Compare_Archive_Samba_Dataset_Directory_Missing
        }

        public enum PurgePolicyConstants
        {
            /// <summary>
            /// Purge large instrument files plus files over 2 MB in size
            /// </summary>
            Auto = 0,

            /// <summary>
            /// Purge all except the QC directory
            /// </summary>
            PurgeAllExceptQC = 1,

            /// <summary>
            /// Purge everything
            /// </summary>
            PurgeAll = 2
        }

        public struct udtDatasetInfoType
        {
            public string DatasetName;
            public string DatasetDirectoryName;
            public string Instrument;
            public string YearQuarter;

            /// <summary>
            /// Full path to the dataset directory on the storage server
            /// </summary>
            public string DatasetDirectoryPath;

            public PurgePolicyConstants PurgePolicy;
            public string RawDataType;
        }

        private readonly MgrSettings mMgrParams;
        private readonly bool mClientPerspective;

        private string mMD5ResultsFileDatasetName = string.Empty;
        private string mMD5ResultsFilePath = string.Empty;

        /// <summary>
        /// Tracks the full path to a file as the key and the MD5 or SHA-1 hash as the value
        /// </summary>
        /// <remarks>
        /// File paths are not case sensitive
        /// MD5 hash values are 32 characters long
        /// SHA-1 hash values are 40 characters long
        /// </remarks>
        private readonly Dictionary<string, HashInfo> mHashFileContents;

        private string mLastMD5WarnDataset = string.Empty;

        private readonly IDBTools mDMSProcedureExecutor;

        public bool PreviewMode { get; set; }

        public bool TraceMode { get; set; }

        public StorageOperations(MgrSettings mgrParams)
        {
            mMgrParams = mgrParams;

            mClientPerspective = mMgrParams.GetParam("perspective") == "client";

            // This Connection String points to the DMS5 database
            var connectionString = mMgrParams.GetParam("ConnectionString");

            mDMSProcedureExecutor = DbToolsFactory.GetDBTools(connectionString);

            mHashFileContents = new Dictionary<string, HashInfo>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Deletes the contents of a dataset directory
        /// </summary>
        /// <param name="purgeParams">Parameters for purge operation</param>
        /// <returns>Enum representing state of task</returns>
        public EnumCloseOutType PurgeDataset(ITaskParams purgeParams)
        {
            string datasetPathSamba;

            var udtDatasetInfo = new udtDatasetInfoType
            {
                DatasetName = purgeParams.GetParam("dataset"),
                DatasetDirectoryName = purgeParams.GetParam("Folder"),
                Instrument = purgeParams.GetParam("Instrument"),
                YearQuarter = purgeParams.GetParam("DatasetYearQuarter"),
                DatasetDirectoryPath = string.Empty,
                PurgePolicy = GetPurgePolicyEnum(purgeParams.GetParam("PurgePolicy")),
                RawDataType = purgeParams.GetParam("RawDataType")
            };

            // Get path to dataset directory on server
            {
                if (mClientPerspective)
                {
                    // Manager is running on a client
                    udtDatasetInfo.DatasetDirectoryPath = purgeParams.GetParam("StorageVolExternal");
                }
                else
                {
                    //Manager is running on storage server
                    udtDatasetInfo.DatasetDirectoryPath = purgeParams.GetParam("StorageVol");
                }
                udtDatasetInfo.DatasetDirectoryPath = Path.Combine(udtDatasetInfo.DatasetDirectoryPath, purgeParams.GetParam("storagePath"));
                udtDatasetInfo.DatasetDirectoryPath = Path.Combine(udtDatasetInfo.DatasetDirectoryPath, udtDatasetInfo.DatasetDirectoryName);

                // Get path to dataset directory in archive
                datasetPathSamba = Path.Combine(purgeParams.GetParam("SambaStoragePath"), udtDatasetInfo.DatasetDirectoryName);
            }

            ReportStatus("Verifying integrity vs. archive, dataset " + udtDatasetInfo.DatasetDirectoryPath);

            switch (CompareDatasetDirectories(udtDatasetInfo, datasetPathSamba, out var lstServerFilesToPurge, out var lstJobsToPurge))
            {
                case ArchiveCompareResults.Compare_Equal:
                    // Everything matches up
                    break;

                case ArchiveCompareResults.Compare_Storage_Server_Directory_Missing:
                    // Confirm that the share for the dataset actual exists on the storage server
                    if (ValidateDatasetShareExists(udtDatasetInfo.DatasetDirectoryPath))
                    {
                        // Share exists; return Failed since we likely need to update the database
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    // Share is missing on the storage server
                    return EnumCloseOutType.CLOSEOUT_DRIVE_MISSING;

                case ArchiveCompareResults.Compare_Error:
                    // Unable to perform comparison operation; set purge task failed
                    //	Error was logged during comparison
                    return EnumCloseOutType.CLOSEOUT_FAILED;

                case ArchiveCompareResults.Compare_Not_Equal_or_Missing:
                    // Server/Archive mismatch; an archive update is required before purging
                    UpdateMD5ResultsFile(udtDatasetInfo);
                    return EnumCloseOutType.CLOSEOUT_UPDATE_REQUIRED;

                case ArchiveCompareResults.Compare_Archive_Samba_Share_Missing:
                    // Archive share is missing
                    // Newer instruments will not have directories on the Samba share because all of their data is in MyEMSL
                    return EnumCloseOutType.CLOSEOUT_ARCHIVE_OFFLINE;

                case ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing:
                    // Dataset directory not found in the archive
                    // This is typically a directory permissions error and we thus do not want to re-archive the directory, since any newly archived files would still be inaccessible
                    return EnumCloseOutType.CLOSEOUT_DATASET_DIRECTORY_MISSING_IN_ARCHIVE;

                default:
                    // Unrecognized result code
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (lstServerFilesToPurge.Count == 0)
            {
                // Nothing was found to purge.
                var msg = "No purgeable data found for dataset " + udtDatasetInfo.DatasetName +
                    ", purge policy = " + GetPurgePolicyDescription(udtDatasetInfo.PurgePolicy);

                switch (udtDatasetInfo.PurgePolicy)
                {
                    case PurgePolicyConstants.Auto:
                        ReportStatus(msg);
                        return EnumCloseOutType.CLOSEOUT_PURGE_AUTO;

                    case PurgePolicyConstants.PurgeAllExceptQC:
                        ReportStatus(msg);
                        return EnumCloseOutType.CLOSEOUT_PURGE_ALL_EXCEPT_QC;

                    case PurgePolicyConstants.PurgeAll:
                        LogError(msg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;

                    default:
                        LogError(msg);
                        return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            var purgeMessage = "Purging " + lstServerFilesToPurge.Count + " file" + CheckPlural(lstServerFilesToPurge.Count) +
                " for dataset " + udtDatasetInfo.DatasetDirectoryPath;

            var simulateMode = false;
#if !DoDelete
            simulateMode = true;
#endif
            // Purge the dataset directory by deleting contents
            if (PreviewMode)
            {
                ReportStatus("Preview: " + purgeMessage);
            }
            else if (simulateMode)
            {
                ReportStatus("SIMULATE: " + purgeMessage);
            }
            else
            {
                ReportStatus(purgeMessage);
            }

            // This list keeps track of the directories that we are processing
            var lstServerDirectories = new SortedSet<string>();

            var filesDeleted = 0;
            var directoriesDeleted = 0;

            // Delete the files listed in lstServerFilesToPurge
            // If the PurgePolicy is AutoPurge or Delete All Except QC, the files in lstServerFilesToPurge could be a subset of the actual files present
            foreach (var fileToDelete in lstServerFilesToPurge)
            {
                try
                {
                    var fiFile = new FileInfo(fileToDelete);
                    if (fiFile.Directory != null && !lstServerDirectories.Contains(fiFile.Directory.FullName))
                        lstServerDirectories.Add(fiFile.Directory.FullName);

                    if (!fiFile.Exists)
                    {
                        continue;
                    }

                    // This code will only delete files if conditional compilation symbol DoDelete is defined
                    try
                    {
                        if (!PreviewMode && !simulateMode)
                            fiFile.Delete();
                    }
                    catch
                    {
                        // Update the ReadOnly flag, then try again
                        if ((fiFile.Attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                            fiFile.Attributes &= ~FileAttributes.ReadOnly;

                        try
                        {
                            if (!PreviewMode && !simulateMode)
                                fiFile.Delete();
                        }
                        catch
                        {
                            // Perform garbage collection, then try again
                            PRISM.AppUtils.GarbageCollectNow();

                            if (!PreviewMode && !simulateMode)
                                fiFile.Delete();
                        }
                    }
                    filesDeleted++;
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting file " + fileToDelete, ex);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Log debug message
            purgeMessage = filesDeleted + " file" + CheckPlural(filesDeleted);

            if (PreviewMode)
                LogDebug("Previewed deletion of " + purgeMessage);
            else if (simulateMode)
                LogDebug("Simulated deletion of " + purgeMessage);
            else
                ReportStatus("Deleted " + purgeMessage);

            // Look for empty directories that can now be deleted
            foreach (var serverDirectory in lstServerDirectories)
            {
                try
                {
                    if (serverDirectory != udtDatasetInfo.DatasetDirectoryPath)
                    {
                        // Note that this function will only delete the directory if conditional compilation symbol DoDelete is defined and if the directory is empty
                        DeleteDirectoryIfEmpty(serverDirectory, ref directoriesDeleted);
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting directory " + serverDirectory, ex);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Log debug message
            LogDebug("Deleted " + directoriesDeleted + " empty directory" + CheckPlural(directoriesDeleted));

            // Delete the dataset directory if it is empty
            bool datasetDirectoryDeleted;
            try
            {
                // Note that this function will only delete the directory if conditional compilation symbol DoDelete is defined and if the directory is empty
                datasetDirectoryDeleted = DeleteDirectoryIfEmpty(udtDatasetInfo.DatasetDirectoryPath, ref directoriesDeleted);
            }
            catch (Exception ex)
            {
                LogError("Exception deleting dataset directory " + udtDatasetInfo.DatasetDirectoryPath, ex);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Log debug message
            LogDebug("Purged files and directories from dataset directory " + udtDatasetInfo.DatasetDirectoryPath);

            // Mark the jobs in lstJobsToPurge as purged
            MarkPurgedJobs(lstJobsToPurge);

            // If we got to here, then log success and exit
            purgeMessage = "Purged dataset " + udtDatasetInfo.DatasetName + ", purge policy = " + GetPurgePolicyDescription(udtDatasetInfo.PurgePolicy);
            if (datasetDirectoryDeleted)
            {
                purgeMessage += ", Dataset directory deleted since now empty";
                udtDatasetInfo.PurgePolicy = PurgePolicyConstants.PurgeAll;
            }

            if (PreviewMode)
            {
                purgeMessage = "Preview: " + purgeMessage;
            }
            else if (simulateMode)
            {
                purgeMessage = "SIMULATE: " + purgeMessage;
            }

            ReportStatus(purgeMessage);

            switch (udtDatasetInfo.PurgePolicy)
            {
                case PurgePolicyConstants.Auto:
                    return EnumCloseOutType.CLOSEOUT_PURGE_AUTO;

                case PurgePolicyConstants.PurgeAllExceptQC:
                    return EnumCloseOutType.CLOSEOUT_PURGE_ALL_EXCEPT_QC;

                case PurgePolicyConstants.PurgeAll:
                    return EnumCloseOutType.CLOSEOUT_SUCCESS;

                default:
                    LogError("Unrecognized purge policy");
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Returns "s" if the value is 0 or greater than 1
        /// Returns "" if the value is 1
        /// </summary>
        /// <param name="value"></param>
        private string CheckPlural(int value)
        {
            if (value == 1)
                return string.Empty;
            return "s";
        }

        /// <summary>
        /// A file was not found in the archive.  Check whether the dataset has any files available via Samba
        /// </summary>
        /// <param name="sambaDatasetNamePath">Samba path for the dataset</param>
        /// <param name="serverFilePath">File path that was not found (included in a log message)</param>
        /// <returns>Comparison result</returns>
        private ArchiveCompareResults CheckSambaPathAvailability(string sambaDatasetNamePath, string serverFilePath)
        {
            // Look for \\agate.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2013_2\DatasetName
            //       or \\agate.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2013_2\
            //       or \\agate.emsl.pnl.gov\dmsarch\LTQ_Orb_3\
            //       or \\agate.emsl.pnl.gov\dmsarch\

            if (!ValidateDatasetShareExists(sambaDatasetNamePath))
            {
                LogWarning("  Archive not found via samba path: " + sambaDatasetNamePath);
                return ArchiveCompareResults.Compare_Archive_Samba_Share_Missing;
            }

            // Make sure the archive directory has at least one file
            // If it doesn't have any files, then we could have a permissions error,
            // or we could be dealing with an instrument whose files are only in MyEMSL

            var datasetDirectory = new DirectoryInfo(sambaDatasetNamePath);

            int fileCount;
            try
            {
                if (!datasetDirectory.Exists)
                {
                    LogDebug("  Dataset not in archive (" + sambaDatasetNamePath + ")");
                    return ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing;
                }
                fileCount = datasetDirectory.GetFiles().Length;
            }
            catch (AccessViolationException)
            {
                LogWarning("  Dataset directory in archive is not accessible, likely a permissions error: " + sambaDatasetNamePath);
                return ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing;
            }
            catch (UnauthorizedAccessException)
            {
                LogWarning("  Dataset directory in archive is not accessible, likely a permissions error: " + sambaDatasetNamePath);
                return ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing;
            }
            catch (Exception ex)
            {
                LogWarning("  Exception examining Dataset directory in archive (" + sambaDatasetNamePath + "): " + ex.Message);
                return ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing;
            }

            if (fileCount > 0)
            {
                // The directory exists in the archive, but the file in question was not there

                LogWarning("  Update required. Server file not found in archive: " + serverFilePath);
                return ArchiveCompareResults.Compare_Not_Equal_or_Missing;
            }

            LogWarning("  Dataset directory in archive is empty (either a permissions error or the dataset is only in MyEMSL): " + sambaDatasetNamePath);
            return ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing;
        }

        /// <summary>
        /// Compares the contents of two dataset directories
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="sambaDatasetNamePath">Location of dataset directory in archive (samba)</param>
        /// <param name="serverFilesToPurge"></param>
        /// <param name="lstJobsToPurge"></param>
        /// <returns>Comparison result</returns>
        public ArchiveCompareResults CompareDatasetDirectories(udtDatasetInfoType udtDatasetInfo, string sambaDatasetNamePath,
            out SortedSet<string> serverFilesToPurge,
            out List<int> lstJobsToPurge)
        {
            serverFilesToPurge = new SortedSet<string>();
            lstJobsToPurge = new List<int>();

            // Set this to true for now
            var compResultOverall = ArchiveCompareResults.Compare_Equal;

            // Verify server dataset directory exists. If it doesn't, either we're getting Access Denied or the directory was manually purged
            var datasetDirectory = new DirectoryInfo(udtDatasetInfo.DatasetDirectoryPath);
            if (!datasetDirectory.Exists)
            {
                LogError("Directory " + udtDatasetInfo.DatasetDirectoryPath + " not found; " +
                         "either the directory was manually purged or Access is Denied");
                return ArchiveCompareResults.Compare_Storage_Server_Directory_Missing;
            }

            // First look for this dataset's files in MyEMSL
            // Next append any files visible using Samba (at \\agate.emsl.pnl.gov\dmsarch\)
            var filesInMyEMSL = FindFilesInMyEMSL(udtDatasetInfo.DatasetName, out var queryException);

            if (filesInMyEMSL.Count == 0)
            {
                // Verify Samba dataset directory exists
                if (!Directory.Exists(sambaDatasetNamePath))
                {
                    string msg;

                    if (queryException)
                    {
                        msg = "Query exception contacting MyEMSL; unable to verify files prior to purge";
                    }
                    else
                    {
                        msg = "Dataset not in MyEMSL and directory " + sambaDatasetNamePath +
                              " not found; unable to verify files prior to purge";
                    }

                    LogError(msg);

                    // Check whether the parent directory exists
                    if (ValidateDatasetShareExists(sambaDatasetNamePath, 2))
                        return ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing;

                    return ArchiveCompareResults.Compare_Archive_Samba_Share_Missing;
                }
            }

            // If the dataset directory is empty yet the parent directory exists, then assume it was manually purged; just update the database
            if (datasetDirectory.GetFileSystemInfos().Length == 0 && datasetDirectory.Parent?.Exists == true)
            {
                LogWarning("Directory " + udtDatasetInfo.DatasetDirectoryPath + " is empty; assuming manually purged");
                return ArchiveCompareResults.Compare_Equal;
            }

            // Find files to purge based on the purge policy
            var purgeableFileSearcher = new PurgeableFileSearcher();
            serverFilesToPurge = purgeableFileSearcher.FindDatasetFilesToPurge(datasetDirectory, udtDatasetInfo, out lstJobsToPurge);

            var mismatchMessage = string.Empty;

            // Populate a dictionary with the relative paths and hash values in filesInMyEMSL
            // The same file could be present in filesInMyEMSL if different versions of the file were pushed into MyEMSL at different times

            // Dictionary dctFilesInMyEMSL tracks the newest version of each file
            // Keys are relative file paths (not case sensitive, Windows slashes)
            // Values are KeyValuePairs where keys are MyEMSL Submission timestamp and values are SHA-1 Hash

            var dctFilesInMyEMSL = new Dictionary<string, KeyValuePair<DateTime, string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var item in filesInMyEMSL)
            {
                if (dctFilesInMyEMSL.TryGetValue(item.RelativePathWindows, out var existingEntry))
                {
                    if (item.SubmissionTimeValue > existingEntry.Key)
                    {
                        // Current item is newer than the item in the dictionary
                        dctFilesInMyEMSL[item.RelativePathWindows] = new KeyValuePair<DateTime, string>(item.SubmissionTimeValue, item.Sha1Hash);
                    }
                }
                else
                {
                    dctFilesInMyEMSL.Add(item.RelativePathWindows, new KeyValuePair<DateTime, string>(item.SubmissionTimeValue, item.Sha1Hash));
                }
            }

            // Loop through the file list, checking for archive copies and comparing if archive copy present
            // We need to generate a hash for all of the files so that we can remove invalid lines from mHashFileContents if a hash mismatch is present
            foreach (var serverFilePath in serverFilesToPurge)
            {
                // Determine if file exists in archive

                var comparisonResult = ArchiveCompareResults.Compare_Not_Equal_or_Missing;

                // First check MyEMSL
                var fileInMyEMSL = false;
                if (dctFilesInMyEMSL.Count > 0)
                {
                    comparisonResult = CompareFileUsingMyEMSLInfo(serverFilePath, udtDatasetInfo, dctFilesInMyEMSL, out fileInMyEMSL);
                }

                if (!fileInMyEMSL)
                {
                    // Look for the file using Samba
                    comparisonResult = CompareFileUsingSamba(sambaDatasetNamePath, serverFilePath, udtDatasetInfo, datasetDirectory);
                }

                switch (comparisonResult)
                {
                    case ArchiveCompareResults.Compare_Equal:
                        continue;

                    case ArchiveCompareResults.Compare_Not_Equal_or_Missing or
                        ArchiveCompareResults.Compare_Archive_Samba_Share_Missing or
                        ArchiveCompareResults.Compare_Archive_Samba_Dataset_Directory_Missing:
                    {
                        // An update is required
                        if (string.IsNullOrEmpty(mismatchMessage) || compResultOverall != ArchiveCompareResults.Compare_Not_Equal_or_Missing)
                            mismatchMessage = "  Update required. Server file " + serverFilePath + " doesn't match copy in MyEMSL or in the archive";

                        compResultOverall = ArchiveCompareResults.Compare_Not_Equal_or_Missing;
                        continue;
                    }

                    case ArchiveCompareResults.Compare_Error:
                        return comparisonResult;

                    default:
                        compResultOverall = ArchiveCompareResults.Compare_Not_Equal_or_Missing;
                        break;
                }
            }

            switch (compResultOverall)
            {
                case ArchiveCompareResults.Compare_Equal:
                    // Everything matches up
                    break;

                case ArchiveCompareResults.Compare_Not_Equal_or_Missing:
                    LogWarning(mismatchMessage);
                    break;
            }

            return compResultOverall;
        }

        private List<MyEMSLReader.ArchivedFileInfo> FindFilesInMyEMSL(string datasetName, out bool queryException)
        {
            queryException = false;

            try
            {
                var reader = new MyEMSLReader.Reader
                {
                    IncludeAllRevisions = false,
                    ReportMetadataURLs = TraceMode,
                    ThrowErrors = true,
                    TraceMode = TraceMode
                };

                // Attach events
                reader.DebugEvent += Reader_DebugEvent;
                reader.ErrorEvent += Reader_ErrorEvent;
                reader.StatusEvent += Reader_MessageEvent;
                reader.ProgressUpdate += Reader_ProgressEvent;

                return reader.FindFilesByDatasetName(datasetName);
            }
            catch (Exception ex)
            {
                LogError("Exception in FindFilesInMyEMSL", ex);
                queryException = true;
            }

            return new List<MyEMSLReader.ArchivedFileInfo>();
        }

        private ArchiveCompareResults CompareFileUsingMyEMSLInfo(
            string serverFilePath,
            udtDatasetInfoType udtDatasetInfo,
            IReadOnlyDictionary<string, KeyValuePair<DateTime, string>> dctFilesInMyEMSL,
            out bool fileInMyEMSL)
        {
            fileInMyEMSL = false;

            // Convert the file name on the storage server to its equivalent relative path
            var relativeFilePath = ConvertServerPathToArchivePath(udtDatasetInfo.DatasetDirectoryPath, string.Empty, serverFilePath);
            relativeFilePath = relativeFilePath.TrimStart('\\');

            if (relativeFilePath.Length == 0)
            {
                LogError("File name not returned when converting from server path to relative path for file" + serverFilePath);
                return ArchiveCompareResults.Compare_Error;
            }

            if (relativeFilePath == PATH_CONVERSION_ERROR)
            {
                // Error was logged by called function, so just return
                return ArchiveCompareResults.Compare_Error;
            }

            var serverFile = new FileInfo(serverFilePath);
            if (!serverFile.Exists)
            {
                LogError("File not found, which is unexpected since this program found this file just recently, cannot compare to MyEMSL Info: " + serverFile.FullName);
                return ArchiveCompareResults.Compare_Not_Equal_or_Missing;
            }

            if (serverFile.Length == 0)
            {
                // MyEMSL does not let us push zero-byte files into MyEMSL
                // Treat zero-byte files as being "in" MyEMSL

                if (TraceMode)
                {
                    LogDebug(string.Format(
                        "{0} is a zero-byte file; ignoring since cannot store empty files in MyEMSL", relativeFilePath));
                }

                fileInMyEMSL = true;
                return ArchiveCompareResults.Compare_Equal;
            }

            // Look for this file in dctFilesInMyEMSL
            if (!dctFilesInMyEMSL.TryGetValue(relativeFilePath, out var archiveFileInfo))
            {
                if (TraceMode)
                {
                    LogDebug(
                        string.Format("{0} not found in MyEMSL", relativeFilePath));
                }

                return ArchiveCompareResults.Compare_Not_Equal_or_Missing;
            }

            if (TraceMode)
            {
                LogDebug(string.Format("Computing SHA-1 hash for {0} file {1}",
                        PRISM.FileTools.BytesToHumanReadable(serverFile.Length), relativeFilePath));
            }

            fileInMyEMSL = true;
            var serverFileHash = Pacifica.Core.Utilities.GenerateSha1Hash(serverFile);

            // Compute the SHA-1 hash value of the file
            if (string.Equals(serverFileHash, archiveFileInfo.Value))
            {
                if (TraceMode)
                {
                    LogDebug(" ... hash matches MyEMSL");
                }

                return ArchiveCompareResults.Compare_Equal;
            }

            LogDebug(string.Format(" ... hashes do not match: {0} locally vs. {1} in MyEMSL", serverFileHash, archiveFileInfo.Value));

            return ArchiveCompareResults.Compare_Not_Equal_or_Missing;
        }

        private ArchiveCompareResults CompareFileUsingSamba(
            string sambaDatasetNamePath,
            string serverFilePath,
            udtDatasetInfoType udtDatasetInfo,
            FileSystemInfo datasetDirectory)
        {
            // Convert the file name on the storage server to its equivalent in the archive
            var archFilePath = ConvertServerPathToArchivePath(udtDatasetInfo.DatasetDirectoryPath, sambaDatasetNamePath, serverFilePath);
            if (archFilePath.Length == 0)
            {
                LogError("File name not returned when converting from server path to archive path for file" + serverFilePath);
                return ArchiveCompareResults.Compare_Error;
            }

            if (archFilePath == PATH_CONVERSION_ERROR)
            {
                // Error was logged by called function, so just return
                return ArchiveCompareResults.Compare_Error;
            }

            var fiArchiveFile = new FileInfo(archFilePath);

            if (!fiArchiveFile.Exists)
            {
                // File doesn't exist in archive
                // The archive could be offline or the file could be stored in MyEMSL
                // All data added to DMS after September 2013 will only be in MyEMSL

                return CheckSambaPathAvailability(sambaDatasetNamePath, serverFilePath);
            }

            // File exists in archive, so compare the server and archive versions
            var comparisonResult = CompareTwoFiles(serverFilePath, archFilePath, udtDatasetInfo);

            if (comparisonResult == ArchiveCompareResults.Compare_Equal ||
                comparisonResult == ArchiveCompareResults.Compare_Not_Equal_or_Missing)
            {
                return comparisonResult;
            }

            if (comparisonResult == ArchiveCompareResults.Hash_Not_Found_For_File)
            {
                // If this file is over AGED_FILE_DAYS days old and is in a subdirectory then only compare file dates
                // If the file in the archive is newer than this file, then assume the archive copy is valid
                // Prior to January 2012 we would assume the files are not equal (since no hash) and would not purge this dataset
                // Due to the slow speed of restoring files to tape we have switched to simply comparing dates
                //
                // In June 2012 we changed AGED_FILE_DAYS from 240 to 45 days since the archive retention period has become quite short

                const int AGED_FILE_DAYS = 45;

                var fiServerFile = new FileInfo(serverFilePath);
                var assumeEqual = false;
                var fileAgeDays = DateTime.UtcNow.Subtract(fiServerFile.LastWriteTimeUtc).TotalDays;

                if (fileAgeDays >= AGED_FILE_DAYS ||
                    fileAgeDays >= 30 && datasetDirectory.Name.StartsWith("blank", StringComparison.OrdinalIgnoreCase))
                {
                    if (fiServerFile.Length == fiArchiveFile.Length && fiServerFile.LastWriteTimeUtc <= fiArchiveFile.LastWriteTimeUtc)
                    {
                        // Copy in archive is the same size and same date (or newer)
                        assumeEqual = true;
                    }
                }

                if (assumeEqual)
                {
                    LogDebug("    archive file size match: " + fiServerFile.FullName.Replace(datasetDirectory.FullName, "").Substring(1));
                    return ArchiveCompareResults.Compare_Equal;
                }

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (comparisonResult == ArchiveCompareResults.Hash_Not_Found_For_File)
                {
                    return comparisonResult;
                }

                // This code should never be reached
                LogError("Logic bug, CompRes = " + comparisonResult + " but should be ArchiveCompareResults.Hash_Not_Found_For_File");
                return ArchiveCompareResults.Compare_Error;
            }

            //There was a problem with the file comparison. Error message has already been logged, so just exit
            return ArchiveCompareResults.Compare_Error;
        }

        /// <summary>
        /// Converts the dataset path on the server to a path in the archive
        /// </summary>
        /// <param name="datasetPathSvr">Dataset path on server</param>
        /// <param name="datasetPathArch">Dataset path on archive</param>
        /// <param name="inpFileName">Name of the file whose path is being converted</param>
        /// <returns>Full archive path to file</returns>
        private string ConvertServerPathToArchivePath(string datasetPathSvr, string datasetPathArch, string inpFileName)
        {
            // Convert by replacing storage server path with archive path
            try
            {
                if (inpFileName.Contains(datasetPathSvr))
                    return inpFileName.Replace(datasetPathSvr, datasetPathArch);

                var charIndex = inpFileName.IndexOf(datasetPathSvr, StringComparison.InvariantCultureIgnoreCase);

                if (charIndex >= 0)
                {
                    var newPath = string.Empty;
                    if (charIndex > 0)
                        newPath = inpFileName.Substring(0, charIndex);

                    newPath += datasetPathArch + inpFileName.Substring(charIndex + datasetPathSvr.Length);
                    return newPath;
                }

                LogError("Error in ConvertServerPathToArchivePath: File path '" + inpFileName + "' does not contain dataset path '" + datasetPathSvr + "'");
                return PATH_CONVERSION_ERROR;
            }
            catch (Exception ex)
            {
                LogError("Exception converting server path to archive path for file " + datasetPathSvr, ex);
                return PATH_CONVERSION_ERROR;
            }
        }

        /// <summary>
        /// Compares two files via MD5 hash
        /// </summary>
        /// <param name="serverFile">First file to compare</param>
        /// <param name="archiveFile">Second file to compare</param>
        /// <param name="udtDatasetInfo">Dataset Info</param>
        /// <returns>Enum containing compare results</returns>
        private ArchiveCompareResults CompareTwoFiles(string serverFile, string archiveFile, udtDatasetInfoType udtDatasetInfo)
        {
            LogDebug("Comparing file " + serverFile + " to file " + archiveFile);

            // Get hash for archive file
            var archiveFileHash = GetArchiveFileHash(serverFile, udtDatasetInfo, out var filePathInDictionary);
            if (string.IsNullOrEmpty(archiveFileHash))
            {
                // There was a problem. Description has already been logged
                return ArchiveCompareResults.Compare_Error;
            }

            if (archiveFileHash == WAITING_FOR_HASH_FILE)
            {
                // There is no hash file.
                // If this is an aged, non-critical file, we'll ignore it if the file sizes are the same and the file date/time in the archive is newer than the local copy
                return ArchiveCompareResults.Hash_Not_Found_For_File;
            }

            if (archiveFileHash == HASH_NOT_FOUND)
            {
                // There is a hash file, but no line exists for serverFile.
                // If this is an aged, non-critical file, we'll ignore it if the file sizes are the same and the file date/time in the archive is newer than the local copy
                return ArchiveCompareResults.Hash_Not_Found_For_File;
            }

            // Get hash for server file
            string serverFileHash;
            if (archiveFileHash.Length < 40)
            {
                // Compute the MD5 hash
                serverFileHash = GenerateMD5HashFromFile(serverFile);
            }
            else
            {
                // This file is in MyEMSL
                // We should have already examined this file in function CompareFileUsingMyEMSLInfo
                LogError("File '" + serverFile + "' has MyEMSL info in the MD5 results file, but was not found using the MyEMSLReader; " +
                         "this likely indicates a problem with Elastic Search");
                return ArchiveCompareResults.Compare_Error;
            }

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

            if (mHashFileContents.TryGetValue(filePathInDictionary, out var hashEntry))
            {
                // Update the cached hash value to #HashMismatch#
                hashEntry.HashCode = HASH_MISMATCH;
            }

            //Files not equal
            return ArchiveCompareResults.Compare_Not_Equal_or_Missing;
        }

        /// <summary>
        /// Delete the given directory if it is empty (no files, and all subdirectories are empty)
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <param name="directoriesDeleted"></param>
        /// <returns>True if the directory was empty and was deleted; otherwise false</returns>
        private bool DeleteDirectoryIfEmpty(string directoryPath, ref int directoriesDeleted)
        {
            var targetDirectory = new DirectoryInfo(directoryPath);

            if (!targetDirectory.Exists)
            {
                return false;
            }

            if (targetDirectory.GetFiles("*.*", SearchOption.AllDirectories).Length == 0)
            {
#if !DoDelete
                directoriesDeleted++;
                return true;
#endif

                // This code will only be reached if conditional compilation symbol DoDelete is defined
                DeleteDirectoryRecurse(targetDirectory.FullName);
                directoriesDeleted++;

                return true;
            }

            return false;
        }

        /// <summary>
        /// Deletes a directory, including all files and subdirectories
        /// Assures that the ReadOnly bit is turned off for each directory
        /// </summary>
        /// <param name="directoryPath"></param>
        private void DeleteDirectoryRecurse(string directoryPath)
        {
#if !DoDelete
            // Debug mode: don't actually delete anything.
            return;
#endif
            var targetDirectory = new DirectoryInfo(directoryPath);

            if (!targetDirectory.Exists)
            {
                // Directory not found; nothing to delete
                return;
            }

            foreach (var subdirectory in targetDirectory.GetDirectories())
            {
                // Check whether the subdirectory is marked as Read-Only
                if ((subdirectory.Attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                    subdirectory.Attributes &= ~FileAttributes.ReadOnly;

                DeleteDirectoryRecurse(subdirectory.FullName);
            }

            try
            {
                if (PreviewMode)
                    Console.WriteLine("Preview: Delete " + targetDirectory.FullName);
                else
                    targetDirectory.Delete(true);
            }
            catch
            {
                // The directory might have read-only files
                // Manually delete each file
                DeleteFilesCheckReadonly(targetDirectory);
                if (PreviewMode)
                    Console.WriteLine("Preview: Delete " + targetDirectory.FullName);
                else
                    targetDirectory.Delete(true);
            }
        }

        /// <summary>
        /// Deletes all files in a directory, assuring that the ReadOnly bit is turned off for each file
        /// </summary>
        /// <param name="targetDirectory"></param>
        private void DeleteFilesCheckReadonly(DirectoryInfo targetDirectory)
        {
            foreach (var targetFile in targetDirectory.GetFiles("*", SearchOption.AllDirectories))
            {
                if ((targetFile.Attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                    targetFile.Attributes &= ~FileAttributes.ReadOnly;

                targetFile.Delete();
            }
        }

        /// <summary>
        /// Find out if there's an MD5 results file for this dataset
        /// That file was previously created by the stagemd5 hash process in the archive
        /// It is now created when a DatasetArchive or ArchiveUpdate task uploads files to MyEMSL
        /// </summary>
        /// <param name="udtDatasetInfo"></param>
        /// <returns>
        /// Full path to an MD5 results file, for example
        /// \\proto-7\MD5Results\VOrbiETD04\2015_4\results.QC_Shew_15_02_500ng_CID-1_4Nov15_Samwise_15-07-19</returns>
        private string GenerateMD5ResultsFilePath(udtDatasetInfoType udtDatasetInfo)
        {
            var hashFileDirectory = mMgrParams.GetParam("MD5ResultsFolderPath");

            var md5ResultsFilePath = Path.Combine(hashFileDirectory, udtDatasetInfo.Instrument);
            md5ResultsFilePath = Path.Combine(md5ResultsFilePath, udtDatasetInfo.YearQuarter);
            md5ResultsFilePath = Path.Combine(md5ResultsFilePath, RESULT_FILE_NAME_PREFIX + udtDatasetInfo.DatasetName);

            return md5ResultsFilePath;
        }

        /// <summary>
        /// Gets the hash value for a file from the results.DatasetName file in the archive
        /// </summary>
        /// <param name="fileNamePath">File on storage server to find a matching archive hatch for</param>
        /// <param name="udtDatasetInfo">Dataset being purged</param>
        /// <param name="filePathInDictionary"></param>
        /// <returns>MD5 or SHA-1 Hash value for success; otherwise return (HASH) or an empty string</returns>
        private string GetArchiveFileHash(string fileNamePath, udtDatasetInfoType udtDatasetInfo, out string filePathInDictionary)
        {
            // Archive should have a results.DatasetName file for the purge candidate dataset. If present, the file
            // will have pre-calculated hash's for the files to be deleted. The manager will look for this result file,
            // and extract the file hash if found. If the hash file is not found, return an empty string,
            // telling the manager to request result file creation

            filePathInDictionary = string.Empty;

            LogDebug("Getting archive hash for file " + fileNamePath);

            if (!string.IsNullOrEmpty(mMD5ResultsFileDatasetName) &&
                string.Equals(mMD5ResultsFileDatasetName, udtDatasetInfo.DatasetName, StringComparison.InvariantCultureIgnoreCase) &&
                mHashFileContents.Count > 0)
            {
                // Hash file has already been loaded into memory; no need to re-load it
            }
            else
            {
                var hashFileLoaded = LoadMD5ResultsFile(udtDatasetInfo, out var waitingForMD5File);

                if (!hashFileLoaded)
                {
                    if (waitingForMD5File)
                        return WAITING_FOR_HASH_FILE;

                    // Error occurred (and has been logged)
                    return string.Empty;
                }
            }

            // Search the hash file contents for a file that matches the input file
            var filePathUnix = fileNamePath.Replace(@"\", "/");

            var subdirectoryToFind = "/" + udtDatasetInfo.DatasetDirectoryName + "/";
            var fileNameTrimmed = TrimPathAfterSubdirectory(filePathUnix, subdirectoryToFind);

            if (string.IsNullOrEmpty(fileNameTrimmed))
            {
                LogWarning("  Did not find " + subdirectoryToFind + " in path " + filePathUnix + " (original path " + fileNamePath + ")");
            }
            else
            {
                if (!mHashFileContents.TryGetValue(fileNameTrimmed, out var hashInfo))
                {
                    LogDebug("  MD5 hash not found for file " + fileNamePath + " using " + fileNameTrimmed + "; see results file " + mMD5ResultsFilePath);
                    return HASH_NOT_FOUND;
                }

                filePathInDictionary = string.Copy(fileNameTrimmed);
                return hashInfo.HashCode;
            }

            return string.Empty;
        }

        private string GetPurgePolicyDescription(PurgePolicyConstants purgePolicy)
        {
            return purgePolicy switch
            {
                PurgePolicyConstants.Auto => "Auto",
                PurgePolicyConstants.PurgeAllExceptQC => "Auto all except the QC directory",
                PurgePolicyConstants.PurgeAll => "Purge all files and directories",
                _ => "??"
            };
        }

        private PurgePolicyConstants GetPurgePolicyEnum(string purgePolicy)
        {
            if (int.TryParse(purgePolicy, out var purgePolicyValue))
            {
                return purgePolicyValue switch
                {
                    0 => PurgePolicyConstants.Auto,
                    1 => PurgePolicyConstants.PurgeAllExceptQC,
                    2 => PurgePolicyConstants.PurgeAll,
                    _ => PurgePolicyConstants.Auto
                };
            }

            return PurgePolicyConstants.Auto;
        }

        /// <summary>
        /// Loads the MD5 results file for the given dataset into memory
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="waitingForMD5File">Output parameter: true if an MD5 has file was not found</param>
        /// <returns>True if success, false if an error</returns>
        private bool LoadMD5ResultsFile(udtDatasetInfoType udtDatasetInfo, out bool waitingForMD5File)
        {
            mHashFileContents.Clear();

            // Find out if there's an MD5 results file for this dataset
            var md5ResultsFilePath = GenerateMD5ResultsFilePath(udtDatasetInfo);

            waitingForMD5File = false;
            mMD5ResultsFileDatasetName = string.Empty;

            try
            {
                if (!File.Exists(md5ResultsFilePath))
                {
                    // MD5 results file not found
                    if (string.CompareOrdinal(udtDatasetInfo.DatasetName, mLastMD5WarnDataset) != 0)
                    {
                        // Warning not yet posted
                        mLastMD5WarnDataset = string.Copy(udtDatasetInfo.DatasetName);

                        ReportStatus("  MD5 results file not found: " + md5ResultsFilePath);

                        // We stopped creating stage stagemd5 files in January 2016
                        // Thus, the following logic is now disabled

                        // Check to see if a stagemd5 file exists for this dataset.
                        // This is for info only since this program does not create stagemd5 files (the DatasetPurgeArchiveHelper creates them)

                        //var hashFileDirectory = mMgrParams.GetParam("HashFileLocation");

                        //var stagedFileNamePath = Path.Combine(hashFileDirectory, STAGED_FILE_NAME_PREFIX + udtDatasetInfo.DatasetName);
                        //if (File.Exists(stagedFileNamePath))
                        //{
                        //    ReportStatus("  Found stagemd5 file: " + stagedFileNamePath);
                        //}
                        //else
                        //{
                        //    // DatasetPurgeArchiveHelper needs to create a stagemd5 file for this dataset
                        //    // Alternatively, if there are a bunch of stagemd5 files waiting to be processed,
                        //    //   eventually we should get MD5 result files and then we should be able to purge this dataset
                        //    LogWarning("  Stagemd5 file not found");
                        //}

                    }

                    waitingForMD5File = true;
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("  Exception searching for MD5 results file", ex);
                return false;
            }

            LogDebug("MD5 results file for dataset found. File name = " + md5ResultsFilePath);

            // Read in results file
            try
            {
                foreach (var inputLine in File.ReadAllLines(md5ResultsFilePath))
                {
                    // Extract the hash values value from the data line

                    // ReSharper disable CommentTypo

                    // Old data not in MyEMSL:
                    //    MD5Hash<SPACE>ArchiveFilePath
                    //
                    // New data in MyEMSL:
                    //    Sha1Hash<SPACE>MyEMSLFilePath<TAB>MyEMSLID
                    //
                    // The Hash and ArchiveFilePath are separated by a space because that's how Ryan Wright's script reported the results
                    // The FilePath and MyEMSLID are separated by a tab in case the file path contains a space

                    // Examples:
                    //
                    // Old data not in MyEMSL:
                    //    0dcf9d677ac76519ae54c11cc5e10723 /archive/dmsarch/VOrbiETD04/2013_3/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15.raw
                    //    d47aca4d13d0a771900eef1fc7ee53ce /archive/dmsarch/VOrbiETD04/2013_3/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15/QC/index.html
                    //
                    // New data in MyEMSL:
                    //    796d99bcc6f1824dfe1c36cc9a61636dd1b07625 /myemsl/svc-dms/SW_TEST_LCQ/2006_1/SWT_LCQData_300/SIC201309041722_Auto976603/Default_2008-08-22.xml	915636
                    //    70976fbd7088b27a711de4ce6309fbb3739d05f9 /myemsl/svc-dms/SW_TEST_LCQ/2006_1/SWT_LCQData_300/SIC201309041722_Auto976603/SWT_LCQData_300_TIC_Scan.tic	915648

                    // ReSharper restore CommentTypo

                    var lstHashAndPathInfo = inputLine.Split(new[] { ' ' }, 2).ToList();
                    if (lstHashAndPathInfo.Count > 1)
                    {
                        // For the above example, we want to store:
                        // "QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15.raw" and "0dcf9d677ac76519ae54c11cc5e10723" or
                        // "SIC201309041722_Auto976603/Default_2008-08-22.xml"    and "796d99bcc6f1824dfe1c36cc9a61636dd1b07625"

                        var hashCode = lstHashAndPathInfo[0];

                        var lstPathAndFileID = lstHashAndPathInfo[1].Split('\t').ToList();

                        var fileNamePath = lstPathAndFileID[0];

                        var myEmslFileID = string.Empty;
                        if (lstPathAndFileID.Count > 1)
                            myEmslFileID = lstPathAndFileID[1];

                        var subdirectoryToFind = "/" + udtDatasetInfo.DatasetDirectoryName + "/";

                        var fileNameTrimmed = TrimPathAfterSubdirectory(fileNamePath, subdirectoryToFind);

                        if (string.IsNullOrEmpty(fileNameTrimmed))
                        {
                            LogWarning("Did not find " + subdirectoryToFind + " in line " + inputLine + " in results file " + md5ResultsFilePath);
                        }
                        else
                        {
                            var newHashInfo = new HashInfo(hashCode, myEmslFileID);

                            // Results files could have duplicate entries if a file was copied to the archive via FTP and was stored via MyEMSL
                            if (mHashFileContents.ContainsKey(fileNameTrimmed))
                            {
                                // Preferentially use the newer value, unless the older value is a MyEMSL SHA-1 hash but the newer value is an MD5 hash
                                if (!(hashCode.Length < 40 && mHashFileContents[fileNameTrimmed].HashCode.Length >= 40))
                                {
                                    mHashFileContents[fileNameTrimmed] = newHashInfo;
                                }
                            }
                            else
                            {
                                mHashFileContents.Add(fileNameTrimmed, newHashInfo);
                            }
                        }
                    }
                    else
                    {
                        // Invalid line; skip it (but continue parsing the file)
                        LogWarning("Unable to split line " + inputLine + " in results file " + md5ResultsFilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception reading MD5 results file " + md5ResultsFilePath, ex);
                return false;
            }

            // If we get here, then the file has successfully been loaded
            mMD5ResultsFileDatasetName = string.Copy(udtDatasetInfo.DatasetName);
            mMD5ResultsFilePath = string.Copy(md5ResultsFilePath);

            return true;
        }

        /// <summary>
        /// Generates MD5 hash of a file's contents
        /// </summary>
        /// <param name="filePath">Full path to file</param>
        /// <returns>String representation of hash</returns>
        private string GenerateMD5HashFromFile(string filePath)
        {
            byte[] byteHash;

            //Verify input file exists
            if (!File.Exists(filePath))
            {
                LogError("StorageOperations.GenerateMD5HashFromFile; File not found: " + filePath);
                return "";
            }

            LogDebug("Generating MD5 hash for file " + filePath);

            try
            {
                // Open file (as read-only)
                using Stream reader = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

                // Get the file's hash
                var hasher = new MD5CryptoServiceProvider();
                byteHash = hasher.ComputeHash(reader);
            }
            catch (Exception ex)
            {
                LogError("StorageOperations.GenerateMD5HashFromFile; Exception generating hash for file " + filePath, ex);
                return string.Empty;
            }

            // Convert hash array to hex string
            var hashStrBld = new StringBuilder();
            foreach (var t in byteHash)
            {
                hashStrBld.Append(t.ToString("x2"));
            }

            return hashStrBld.ToString();
        }

        /// <summary>
        /// Call DMS to change AJ_Purged to 1 for the jobs in lstJobsToPurge
        /// </summary>
        /// <param name="lstJobsToPurge"></param>
        private void MarkPurgedJobs(IReadOnlyCollection<int> lstJobsToPurge)
        {
            const string SP_MARK_PURGED_JOBS = "mark_purged_jobs";

            if (lstJobsToPurge.Count == 0)
            {
                return;
            }

            // Construct a comma-separated list of jobs
            var jobList = string.Join(", ", lstJobsToPurge);

            var simulateMode = false;
#if !DoDelete
            simulateMode = true;
#endif
            if (PreviewMode || simulateMode)
            {
                var msg = "SIMULATE: call to " + SP_MARK_PURGED_JOBS + " for job" + CheckPlural(lstJobsToPurge.Count) + " " + jobList;
                LogDebug(msg);
                return;
            }

            // Call stored procedure mark_purged_jobs

            const int maxRetryCount = 3;

            var serverType = DbToolsFactory.GetServerTypeFromConnectionString(mDMSProcedureExecutor.ConnectStr);

            //Setup for execution of the stored procedure
            var cmd = mDMSProcedureExecutor.CreateCommand(SP_MARK_PURGED_JOBS, CommandType.StoredProcedure);

            // Define parameter for procedure's return value
            // If querying a Postgres DB, mDMSProcedureExecutor will auto-change "@return" to "_returnCode"
            var returnParam = mDMSProcedureExecutor.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

            mDMSProcedureExecutor.AddParameter(cmd, "@JobList", SqlType.VarChar, 4000, jobList);

            if (serverType == DbServerTypes.PostgreSQL)
            {
                mDMSProcedureExecutor.AddParameter(cmd, "@infoOnly", SqlType.Boolean).Value = false;
            }
            else
            {
                mDMSProcedureExecutor.AddParameter(cmd, "@infoOnly", SqlType.TinyInt).Value = 0;
            }

            //Execute the SP
            mDMSProcedureExecutor.ExecuteSP(cmd, out var errorMessage, maxRetryCount);

            var returnCode = DBToolsBase.GetReturnCode(returnParam);

            if (returnCode == 0)
            {
                ReportStatus("Marked job" + CheckPlural(lstJobsToPurge.Count) + " " + jobs + " as purged");
            }
            else
            {
                var msg = "Error calling stored procedure " + SP_MARK_PURGED_JOBS + " to mark job" + CheckPlural(lstJobsToPurge.Count) + " " + jobs + " as purged";
                if (!string.IsNullOrEmpty(errorMessage))
                    msg += ": " + errorMessage;

                LogError(msg);
            }
        }

        /// <summary>
        /// Looks for subdirectoryToFind in fileNamePath
        /// If found, returns the text that occurs after subdirectoryToFind
        /// If not found, then returns an empty string
        /// </summary>
        /// <param name="fileNamePath"></param>
        /// <param name="subdirectoryToFind"></param>
        private string TrimPathAfterSubdirectory(string fileNamePath, string subdirectoryToFind)
        {
            var startIndex = fileNamePath.IndexOf(subdirectoryToFind, StringComparison.InvariantCultureIgnoreCase);

            if (startIndex < 0)
            {
                // Try again using lowercase
                startIndex = fileNamePath.IndexOf(subdirectoryToFind, StringComparison.InvariantCultureIgnoreCase);
            }

            if (startIndex >= 0)
            {
                if (startIndex + subdirectoryToFind.Length < fileNamePath.Length)
                    return fileNamePath.Substring(startIndex + subdirectoryToFind.Length);

                return string.Empty;
            }

            return string.Empty;
        }

        /// <summary>
        /// Updates the archive hash file for a dataset to only retain lines where the MD5 hash value agree
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset Info</param>
        private void UpdateMD5ResultsFile(udtDatasetInfoType udtDatasetInfo)
        {
            var currentStep = "Start";

            // Find out if there's a master MD5 results file for this dataset
            var md5ResultsFileMasterPath = GenerateMD5ResultsFilePath(udtDatasetInfo);

            if (!File.Exists(md5ResultsFileMasterPath))
            {
                // Master MD5 results file not found; nothing to do
                return;
            }

            // Update the hash file to remove any entries with a hash of HASH_MISMATCH in mHashFileContents
            // These are files for which the MD5 hash of the actual file doesn't match the hash stored in the master MD5 results file,
            //   and we thus need to re-compute the hash using the file in the Archive
            // In theory, before we do this, the Archive Update manager will update the file
            try
            {
                var updatedMD5Info = new List<string>();
                var writeUpdatedMD5Info = false;

                var splitChars = new[] { ' ' };

                var subdirectoryToFind = "/" + udtDatasetInfo.DatasetDirectoryName + "/";

                currentStep = "Read master MD5 results file";

                // Open the master MD5 results file and read each line
                using (var hashFileReader = new StreamReader(new FileStream(md5ResultsFileMasterPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!hashFileReader.EndOfStream)
                    {
                        var inputLine = hashFileReader.ReadLine();
                        if (string.IsNullOrWhiteSpace(inputLine))
                            continue;

                        // Extract the MD5 results value from lineIn
                        // Format is MD5 code, then a space, then a full path to the file
                        // Example:
                        // 2036b65346acd59f3dd044b6a97bf44a /archive/dmsarch/LTQ_Orb_1/2008_1/EIF_Plasma_C_18_10Jan08_Draco_07-12-24/Seq200901221155_Auto362389/EIF_Plasma_C_18_10Jan08_Draco_07-12-24_out.zip

                        var lineParts = inputLine.Split(splitChars, 2);
                        if (lineParts.Length <= 1)
                        {
                            continue;
                        }

                        // Look for the Linux-style file path in mHashFileContents
                        var fileNameTrimmed = TrimPathAfterSubdirectory(lineParts[1], subdirectoryToFind);

                        if (string.IsNullOrEmpty(fileNameTrimmed))
                        {
                            // Did not find in lineParts[1]; this is unexpected
                            // An error should have already been logged when function LoadMD5ResultsFile() parsed this file
                            continue;
                        }

                        if (mHashFileContents.TryGetValue(fileNameTrimmed, out var hashInfo))
                        {
                            // Match found; examine md5HashNew
                            if (string.Equals(hashInfo.HashCode, HASH_MISMATCH))
                            {
                                // Old comment:
                                //   We need the DatasetPurgeArchiveHelper to create a new stagemd5 file that computes a new hash for this file
                                //   Do not include this line in lstUpdatedMD5Info;

                                // New Comment:
                                //   We need to run a new archive update job for this dataset
                                //   The ArchiveVerify tool will run as part of that job and will update the cached hash values

                                writeUpdatedMD5Info = true;
                            }
                            else
                            {
                                // Hash codes match; retain this line
                                updatedMD5Info.Add(inputLine);
                            }
                        }
                        else
                        {
                            // Match not found in mHashFileContents
                            // Retain this line
                            updatedMD5Info.Add(inputLine);
                        }
                    } // while not at EndOfStream
                }

                if (writeUpdatedMD5Info)
                {
                    var md5ResultsFilePathTemp = md5ResultsFileMasterPath + ".updated";

                    currentStep = "Create " + md5ResultsFilePathTemp;
                    using (var hashFileWriter = new StreamWriter(new FileStream(md5ResultsFilePathTemp, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (var outputLine in updatedMD5Info)
                        {
                            hashFileWriter.WriteLine(outputLine);
                        }
                    }

                    System.Threading.Thread.Sleep(100);

                    currentStep = "Overwrite master MD5 results file with " + md5ResultsFilePathTemp;
                    File.Copy(md5ResultsFilePathTemp, md5ResultsFileMasterPath, true);
                    System.Threading.Thread.Sleep(100);

                    currentStep = "Delete " + md5ResultsFilePathTemp;
                    File.Delete(md5ResultsFilePathTemp);

                    ReportStatus("  Updated MD5 results file " + md5ResultsFileMasterPath);
                }
                else
                {
                    LogDebug("MD5 results file does not require updating: " + md5ResultsFileMasterPath);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception updating MD5 results file " + md5ResultsFileMasterPath + "; CurrentStep: " + currentStep, ex);
            }
        }

        /// <summary>
        /// Validate that the share for the dataset actually exists
        /// </summary>
        /// <param name="datasetDirectoryPath"></param>
        /// <param name="maxParentDepth">
        /// Maximum number of parent directories to examine when looking for a valid directory
        /// -1 means parse all parent directories until a valid one is found
        /// </param>
        /// <returns>True if the dataset directory or the share that should have the dataset directory exists, other wise false</returns>
        private bool ValidateDatasetShareExists(string datasetDirectoryPath, int maxParentDepth = -1)
        {
            try
            {
                var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);

                if (datasetDirectory.Exists)
                    return true;

                if (maxParentDepth == 0)
                    return false;

                var parentDepth = 0;

                while (datasetDirectory.Parent != null)
                {
                    datasetDirectory = datasetDirectory.Parent;
                    if (datasetDirectory.Exists)
                        return true;

                    parentDepth++;
                    if (maxParentDepth > -1 && parentDepth > maxParentDepth)
                        break;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception validating that directory " + datasetDirectoryPath + " exists", ex);
                return false;
            }
        }

        private void Reader_DebugEvent(string message)
        {
            LogDebug(message);
        }

        private void Reader_ErrorEvent(string message, Exception ex)
        {
            LogError("MyEMSLReader: " + message);
        }

        private void Reader_MessageEvent(string message)
        {
            ReportStatus(message);
        }

        private void Reader_ProgressEvent(string progressMessage, float percentComplete)
        {
            _ = "Percent complete: " + percentComplete.ToString("0.0") + "%";

            /*
             * Logging of percent progress is disabled since we're only using the Reader to query for file information and not to download files from MyEMSL
             *
            if (e.PercentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
            {
                if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
                {
                    LogDebug(message);
                    mPercentComplete = e.PercentComplete;
                    mLastProgressUpdateTime = DateTime.UtcNow;
                }
            }
            */
        }
    }
}
