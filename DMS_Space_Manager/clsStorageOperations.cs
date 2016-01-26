
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/15/2010
//
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using MyEMSLReader;

namespace Space_Manager
{
    public class clsStorageOperations
    {
        //*********************************************************************************************************
        // Class to perform a purge task and all associated operations
        //**********************************************************************************************************
        #region "Constants"
        const string RESULT_FILE_NAME_PREFIX = "results.";
        // Obsolete: const string STAGED_FILE_NAME_PREFIX = "stagemd5.";
        const string WAITING_FOR_HASH_FILE = "(HASH)";
        const string HASH_NOT_FOUND = "(HASH NOT FOUND)";
        const string HASH_MISMATCH = "#HashMismatch#";

        const string PATH_CONVERSION_ERROR = "Error";

        #endregion

        #region "Enums"

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
            /// The storage server folder was not found
            /// </summary>
            Compare_Storage_Server_Folder_Missing,

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
            /// Folder not found: \\a2.emsl.pnl.gov\dmsarch
            /// </summary>
            Compare_Archive_Samba_Share_Missing,

            /// <summary>
            /// \\a2.emsl.pnl.gov\dmsarch exists but the dataset does not have a folder in the archive
            /// </summary>
            Compare_Archive_Samba_DatasetFolder_Missing
        }

        public enum PurgePolicyConstants
        {
            /// <summary>
            /// Purge large instrument files plus files over 2 MB in size
            /// </summary>
            Auto = 0,

            /// <summary>
            /// Purge all except the QC folder
            /// </summary>
            PurgeAllExceptQC = 1,

            /// <summary>
            /// Purge everything
            /// </summary>
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

            /// <summary>
            /// Folder path of this dataset on the storage server
            /// </summary>
            public string ServerFolderPath;
            public PurgePolicyConstants PurgePolicy;
            public string RawDataType;
        }
        #endregion

        #region "Class variables"

        readonly IMgrParams m_MgrParams;
        readonly bool m_ClientPerspective;

        string m_MD5ResultsFileDatasetName = string.Empty;
        string m_MD5ResultsFilePath = string.Empty;

        /// <summary>
        /// Tracks the full path to a file as the key and the MD5 or Sha-1 hash as the value
        /// </summary>
        /// <remarks>
        /// File paths are not case sensitive
        /// MD5 hash values are 32 characters long
        /// Sha-1 hash values are 40 characters long
        /// </remarks>
        Dictionary<string, clsHashInfo> m_HashFileContents;

        string m_LastMD5WarnDataset = string.Empty;

        #endregion

        #region "Properties"

        protected PRISM.DataBase.clsExecuteDatabaseSP DMSProcedureExecutor;

        #endregion

        #region "Constructors"

        public clsStorageOperations(IMgrParams mgrParams)
        {
            m_MgrParams = mgrParams;

            m_ClientPerspective = (m_MgrParams.GetParam("perspective") == "client");

            // This Connection String points to the DMS5 database
            var connectionString = m_MgrParams.GetParam("ConnectionString");

            DMSProcedureExecutor = new PRISM.DataBase.clsExecuteDatabaseSP(connectionString);
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Deletes the contents of a dataset folder
        /// </summary>
        /// <param name="purgeParams">Parameters for purge operation</param>
        /// <returns>Enum representing state of task</returns>
        public EnumCloseOutType PurgeDataset(ITaskParams purgeParams)
        {
            string datasetPathSamba;

            var udtDatasetInfo = new udtDatasetInfoType
            {
                DatasetName = purgeParams.GetParam("dataset"),
                DatasetFolderName = purgeParams.GetParam("Folder"),
                Instrument = purgeParams.GetParam("Instrument"),
                YearQuarter = purgeParams.GetParam("DatasetYearQuarter"),
                ServerFolderPath = string.Empty,
                PurgePolicy = GetPurgePolicyEnum(purgeParams.GetParam("PurgePolicy")),
                RawDataType = purgeParams.GetParam("RawDataType")
            };

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
                udtDatasetInfo.ServerFolderPath = Path.Combine(udtDatasetInfo.ServerFolderPath, purgeParams.GetParam("storagePath"));
                udtDatasetInfo.ServerFolderPath = Path.Combine(udtDatasetInfo.ServerFolderPath, udtDatasetInfo.DatasetFolderName);

                // Get path to dataset folder in archive
                datasetPathSamba = Path.Combine(purgeParams.GetParam("SambaStoragePath"), udtDatasetInfo.DatasetFolderName);
            }

            var msg = "Verifying integrity vs. archive, dataset " + udtDatasetInfo.ServerFolderPath;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
            Console.WriteLine(msg);

            SortedSet<string> lstServerFilesToPurge;
            List<int> lstJobsToPurge;
            var compResult = CompareDatasetFolders(udtDatasetInfo, datasetPathSamba, out lstServerFilesToPurge, out lstJobsToPurge);

            switch (compResult)
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

                    // Share is missing
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
                    // Newer instruments will not have folders on the Samba share because all of their data is in MyEMSL
                    return EnumCloseOutType.CLOSEOUT_DRIVE_MISSING;

                case ArchiveCompareResults.Compare_Archive_Samba_DatasetFolder_Missing:
                    // Dataset folder not found in the archive
                    // This is typically a folder permissions error and we thus do not want to re-archive the folder, since any newly archived files would still be inaccessible
                    return EnumCloseOutType.CLOSEOUT_DATASET_FOLDER_MISSING_IN_ARCHIVE;

                default:
                    // Unrecognized result code
                    return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if ((lstServerFilesToPurge.Count == 0))
            {
                // Nothing was found to purge.
                msg = "No purgeable data found for dataset " + udtDatasetInfo.DatasetName + ", purge policy = " + GetPurgePolicyDescription(udtDatasetInfo.PurgePolicy);
                Console.WriteLine(msg);

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


            msg = "Purging " + lstServerFilesToPurge.Count + " file" + CheckPlural(lstServerFilesToPurge.Count) + " for dataset " + udtDatasetInfo.ServerFolderPath;

#if DoDelete
            // Purge the dataset folder by deleting contents            
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
            Console.WriteLine(msg);
#else
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "SIMULATE: " + msg);
#endif

            // This list keeps track of the folders that we are processing
            var lstServerFolders = new SortedSet<string>();

            var iFilesDeleted = 0;
            var iFoldersDeleted = 0;

            // Delete the files listed in lstServerFilesToPurge
            // If the PurgePolicy is AutoPurge or Delete All Except QC, the files in lstServerFilesToPurge could be a subset of the actual files present
            foreach (var fileToDelete in lstServerFilesToPurge)
            {
                try
                {
                    var fiFile = new FileInfo(fileToDelete);
                    if (fiFile.Directory != null && !lstServerFolders.Contains(fiFile.Directory.FullName))
                        lstServerFolders.Add(fiFile.Directory.FullName);

                    if (!fiFile.Exists)
                    {
                        continue;
                    }

#if DoDelete
                    // This code will only be reached if conditional compilation symbol DoDelete is defined
                    try
                    {
                        fiFile.Delete();
                    }
                    catch
                    {
                        // Update the ReadOnly flag, then try again
                        if ((fiFile.Attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                            fiFile.Attributes = fiFile.Attributes & ~FileAttributes.ReadOnly;

                        try
                        {
                            fiFile.Delete();
                        }
                        catch
                        {
                            // Perform garbage collection, then try again
                            PRISM.Processes.clsProgRunner.GarbageCollectNow();

                            fiFile.Delete();
                        }

                    }
#endif
                    iFilesDeleted += 1;
                }
                catch (Exception ex)
                {
                    msg = "Exception deleting file " + fileToDelete + "; " + ex.Message;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    Console.WriteLine(msg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Log debug message
            msg = "Deleted " + iFilesDeleted + " file" + CheckPlural(iFilesDeleted);
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            Console.WriteLine(msg);

            // Look for empty folders that can now be deleted
            foreach (var serverFolder in lstServerFolders)
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
                    Console.WriteLine(msg);
                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Log debug message
            msg = "Deleted " + iFoldersDeleted + " empty folder" + CheckPlural(iFoldersDeleted);
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            Console.WriteLine(msg);

            // Delete the dataset folder if it is empty
            bool datasetFolderDeleted;
            try
            {
                // Note that this function will only delete the folder if conditional compilation symbol DoDelete is defined and if the folder is empty
                datasetFolderDeleted = DeleteFolderIfEmpty(udtDatasetInfo.ServerFolderPath, ref iFoldersDeleted);
            }
            catch (Exception ex)
            {
                msg = "Exception deleting dataset folder " + udtDatasetInfo.ServerFolderPath + "; " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                Console.WriteLine(msg);
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Log debug message
            msg = "Purged files and folders from dataset folder " + udtDatasetInfo.ServerFolderPath;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            Console.WriteLine(msg);

            // Mark the jobs in lstJobsToPurge as purged
            MarkPurgedJobs(lstJobsToPurge);

            // If we got to here, then log success and exit
            msg = "Purged dataset " + udtDatasetInfo.DatasetName + ", purge policy = " + GetPurgePolicyDescription(udtDatasetInfo.PurgePolicy);
            if (datasetFolderDeleted)
            {
                msg += ", Dataset folder deleted since now empty";
                udtDatasetInfo.PurgePolicy = PurgePolicyConstants.PurgeAll;
            }

#if !DoDelete
			msg = "SIMULATE: " + msg;
#endif

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
            Console.WriteLine(msg);

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

        }

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
        /// A file was not found in the archive.  Check whether the dataset has any files available via Samba
        /// </summary>
        /// <param name="sambaDatasetNamePath">Samba path for the dataset</param>
        /// <param name="sServerFilePath">File path that was not found (included in a log message)</param>
        /// <returns></returns>
        protected ArchiveCompareResults CheckSambaPathAvailability(string sambaDatasetNamePath, string sServerFilePath)
        {
            string msg;

            // Look for \\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2013_2\DatasetName
            //       or \\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2013_2\
            //       or \\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\
            //       or \\a2.emsl.pnl.gov\dmsarch\

            if (!ValidateDatasetShareExists(sambaDatasetNamePath))
            {
                msg = "  Archive not found via samba path: " + sambaDatasetNamePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                Console.WriteLine(msg);

                return ArchiveCompareResults.Compare_Archive_Samba_Share_Missing;
            }

            // Make sure the archive folder has at least one file
            // If it doesn't have any files, then we could have a permissions error, 
            // or we could be dealing with an instrument whose files are only in MyEMSL

            var diDatasetFolder = new DirectoryInfo(sambaDatasetNamePath);

            int intFileCount;
            try
            {
                intFileCount = diDatasetFolder.GetFiles().Length;
            }
            catch (AccessViolationException)
            {
                msg = "  Dataset folder in archive is not accessible, likely a permissions error: " + sambaDatasetNamePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                Console.WriteLine(msg);
                return ArchiveCompareResults.Compare_Archive_Samba_DatasetFolder_Missing;
            }
            catch (UnauthorizedAccessException)
            {
                msg = "  Dataset folder in archive is not accessible, likely a permissions error: " + sambaDatasetNamePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                Console.WriteLine(msg);
                return ArchiveCompareResults.Compare_Archive_Samba_DatasetFolder_Missing;
            }
            catch (Exception ex)
            {
                msg = "  Exception examining Dataset folder in archive (" + sambaDatasetNamePath + "): " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                Console.WriteLine(msg);
                return ArchiveCompareResults.Compare_Archive_Samba_DatasetFolder_Missing;
            }

            if (intFileCount > 0)
            {
                // The folder exists in the aurora archive, but the file in question was not there 

                msg = "  Update required. Server file not found in archive: " + sServerFilePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                Console.WriteLine(msg);
                return ArchiveCompareResults.Compare_Not_Equal_or_Missing;
            }

            msg = "  Dataset folder in archive is empty (either a permissions error or the dataset is only in MyEMSL): " + sambaDatasetNamePath;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
            Console.WriteLine(msg);
            return ArchiveCompareResults.Compare_Archive_Samba_DatasetFolder_Missing;
        }

        /// <summary>
        /// Compares the contents of two dataset folders
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="sambaDatasetNamePath">Location of dataset folder in archive (samba)</param>
        /// <param name="lstServerFilesToPurge"></param>
        /// <param name="lstJobsToPurge"></param>
        /// <returns></returns>
        public ArchiveCompareResults CompareDatasetFolders(udtDatasetInfoType udtDatasetInfo, string sambaDatasetNamePath,
            out SortedSet<string> lstServerFilesToPurge,
            out List<int> lstJobsToPurge)
        {
            lstServerFilesToPurge = new SortedSet<string>();
            lstJobsToPurge = new List<int>();

            string msg;

            // Set this to true for now
            var eCompResultOverall = ArchiveCompareResults.Compare_Equal;

            // Verify server dataset folder exists. If it doesn't, either we're getting Access Denied or the folder was manually purged
            var diDatasetFolder = new DirectoryInfo(udtDatasetInfo.ServerFolderPath);
            if (!diDatasetFolder.Exists)
            {
                msg = "clsUpdateOps.CompareDatasetFolders, folder " + udtDatasetInfo.ServerFolderPath + " not found; either the folder was manually purged or Access is Denied";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                Console.WriteLine(msg);
                return ArchiveCompareResults.Compare_Storage_Server_Folder_Missing;
            }

            // First look for this dataset's files in MyEMSL
            // Next append any files visible using Samba (at \\a2.emsl.pnl.gov\dmsarch\)
            var lstFilesInMyEMSL = FindFilesInMyEMSL(udtDatasetInfo.DatasetName);

            if (lstFilesInMyEMSL.Count == 0)
            {
                // Verify Samba dataset folder exists
                if (!Directory.Exists(sambaDatasetNamePath))
                {
                    msg = "clsUpdateOps.CompareDatasetFolders, dataset not in MyEMSL and folder " + sambaDatasetNamePath + " not found; unable to verify files prior to purge";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                    Console.WriteLine(msg);

                    // Check whether the parent folder exists
                    if (ValidateDatasetShareExists(sambaDatasetNamePath, 2))
                        return ArchiveCompareResults.Compare_Archive_Samba_DatasetFolder_Missing;

                    return ArchiveCompareResults.Compare_Archive_Samba_Share_Missing;
                }
            }

            // If the dataset folder is empty yet the parent folder exists, then assume it was manually purged; just update the database
            if (diDatasetFolder.GetFileSystemInfos().Length == 0 && diDatasetFolder.Parent != null && diDatasetFolder.Parent.Exists)
            {
                msg = "clsUpdateOps.CompareDatasetFolders, folder " + udtDatasetInfo.ServerFolderPath + " is empty; assuming manually purged";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                Console.WriteLine(msg);
                return ArchiveCompareResults.Compare_Equal;
            }

            // Find files to purge based on the purge policy
            var oPurgeableFileSearcher = new clsPurgeableFileSearcher();
            lstServerFilesToPurge = oPurgeableFileSearcher.FindDatasetFilesToPurge(diDatasetFolder, udtDatasetInfo, out lstJobsToPurge);

            var sMismatchMessage = string.Empty;

            // Populate a dictionary with the relative paths and hash values in lstFilesInMyEMSL
            // File paths are not case sensitive
            var dctFilesInMyEMSL = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            foreach (var item in lstFilesInMyEMSL)
            {
                if (dctFilesInMyEMSL.ContainsKey(item.RelativePathWindows))
                {
                    throw new Exception("lstFilesInMyEMSL has duplicate entries for " + item.RelativePathWindows + "; this indicates a bug in MyEMSLReader");
                }
                dctFilesInMyEMSL.Add(item.RelativePathWindows, item.Sha1Hash);
            }

            // Loop through the file list, checking for archive copies and comparing if archive copy present
            // We need to generate a hash for all of the files so that we can remove invalid lines from m_HashFileContents if a hash mismatch is present
            foreach (var sServerFilePath in lstServerFilesToPurge)
            {
                // Determine if file exists in archive

                var comparisonResult = ArchiveCompareResults.Compare_Not_Equal_or_Missing;

                // First check MyEMSL
                var fileInMyEMSL = false;
                if (dctFilesInMyEMSL.Count > 0)
                {
                    comparisonResult = CompareFileUsingMyEMSLInfo(sServerFilePath, udtDatasetInfo, dctFilesInMyEMSL, out fileInMyEMSL);
                }

                if (!fileInMyEMSL)
                {
                    // Look for the file using Samba
                    comparisonResult = CompareFileUsingSamba(sambaDatasetNamePath, sServerFilePath, udtDatasetInfo, diDatasetFolder);
                }

                if (comparisonResult == ArchiveCompareResults.Compare_Equal)
                    continue;

                if (comparisonResult == ArchiveCompareResults.Compare_Not_Equal_or_Missing)
                {
                    // An update is required
                    if (string.IsNullOrEmpty(sMismatchMessage) || eCompResultOverall != ArchiveCompareResults.Compare_Not_Equal_or_Missing)
                        sMismatchMessage = "  Update required. Server file " + sServerFilePath + " doesn't match copy in MyEMSL or in the archive";

                    eCompResultOverall = ArchiveCompareResults.Compare_Not_Equal_or_Missing;
                    continue;
                }

                if (comparisonResult == ArchiveCompareResults.Compare_Error)
                {
                    return comparisonResult;
                }

            } // foreach File in lstServerFilesToPurge

            switch (eCompResultOverall)
            {
                case ArchiveCompareResults.Compare_Equal:
                    // Everything matches up
                    break;

                case ArchiveCompareResults.Compare_Not_Equal_or_Missing:
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, sMismatchMessage);
                    break;

            }

            return eCompResultOverall;

        }

        protected List<ArchivedFileInfo> FindFilesInMyEMSL(string datasetName)
        {

            try
            {
                var reader = new MyEMSLReader.Reader
                {
                    IncludeAllRevisions = false
                };

                // Attach events
                reader.ErrorEvent += reader_ErrorEvent;
                reader.MessageEvent += reader_MessageEvent;
                reader.ProgressEvent += reader_ProgressEvent;

                var lstFilesInMyEMSL = reader.FindFilesByDatasetName(datasetName);

                return lstFilesInMyEMSL;

            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in FindFilesInMyEMSL", ex);
            }

            return new List<ArchivedFileInfo>();
        }


        protected ArchiveCompareResults CompareFileUsingMyEMSLInfo(
            string sServerFilePath,
            udtDatasetInfoType udtDatasetInfo,
            Dictionary<string, string> dctFilesInMyEMSL,
            out bool fileInMyEMSL)
        {
            var comparisonResult = ArchiveCompareResults.Compare_Not_Equal_or_Missing;

            fileInMyEMSL = false;

            // Convert the file name on the storage server to its equivalent relative path
            var relativeFilePath = ConvertServerPathToArchivePath(udtDatasetInfo.ServerFolderPath, string.Empty, sServerFilePath);
            relativeFilePath = relativeFilePath.TrimStart('\\');

            if (relativeFilePath.Length == 0)
            {
                var msg = "File name not returned when converting from server path to relative path for file" + sServerFilePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return ArchiveCompareResults.Compare_Error;
            }

            if (relativeFilePath == PATH_CONVERSION_ERROR)
            {
                // Error was logged by called function, so just return
                return ArchiveCompareResults.Compare_Error;
            }

            // Look for this file in dctFilesInMyEMSL
            string archiveFileHash;
            if (dctFilesInMyEMSL.TryGetValue(relativeFilePath, out archiveFileHash))
            {

                fileInMyEMSL = true;
                var serverFileHash = Pacifica.Core.Utilities.GenerateSha1Hash(sServerFilePath);

                // Compute the sha-1 hash value of the file
                if (string.Equals(serverFileHash, archiveFileHash))
                {
                    comparisonResult = ArchiveCompareResults.Compare_Equal;
                }
            }

            return comparisonResult;
        }

        protected ArchiveCompareResults CompareFileUsingSamba(
            string sambaDatasetNamePath,
            string sServerFilePath,
            udtDatasetInfoType udtDatasetInfo,
            DirectoryInfo diDatasetFolder)
        {
            string msg;

            // Convert the file name on the storage server to its equivalent in the archive
            var archFilePath = ConvertServerPathToArchivePath(udtDatasetInfo.ServerFolderPath, sambaDatasetNamePath, sServerFilePath);
            if (archFilePath.Length == 0)
            {
                msg = "File name not returned when converting from server path to archive path for file" + sServerFilePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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

                return CheckSambaPathAvailability(sambaDatasetNamePath, sServerFilePath);
            }

            // File exists in archive, so compare the server and archive versions
            var comparisonResult = CompareTwoFiles(sServerFilePath, archFilePath, udtDatasetInfo);

            if (comparisonResult == ArchiveCompareResults.Compare_Equal ||
                comparisonResult == ArchiveCompareResults.Compare_Not_Equal_or_Missing)
                return comparisonResult;

            if (comparisonResult == ArchiveCompareResults.Hash_Not_Found_For_File)
            {

                // If this file is over AGED_FILE_DAYS days old and is in a subfolder then only compare file dates
                // If the file in the archive is newer than this file, then assume the archive copy is valid
                // Prior to January 2012 we would assume the files are not equal (since no hash) and would not purge this dataset
                // Due to the slow speed of restoring files to tape we have switched to simply comparing dates
                //
                // In June 2012 we changed AGED_FILE_DAYS from 240 to 45 days since the archive retention period has become quite short

                const int AGED_FILE_DAYS = 45;

                var fiServerFile = new FileInfo(sServerFilePath);
                var bAssumeEqual = false;
                var dFileAgeDays = DateTime.UtcNow.Subtract(fiServerFile.LastWriteTimeUtc).TotalDays;

                if (dFileAgeDays >= AGED_FILE_DAYS ||
                    dFileAgeDays >= 30 && diDatasetFolder.Name.StartsWith("blank", StringComparison.InvariantCultureIgnoreCase))
                {
                    if (fiServerFile.Length == fiArchiveFile.Length && fiServerFile.LastWriteTimeUtc <= fiArchiveFile.LastWriteTimeUtc)
                    {
                        // Copy in archive is the same size and same date (or newer)
                        bAssumeEqual = true;
                    }

                }

                if (bAssumeEqual)
                {
                    msg = "    archive file size match: " + fiServerFile.FullName.Replace(diDatasetFolder.FullName, "").Substring(1);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    return ArchiveCompareResults.Compare_Equal;
                }

                if (comparisonResult == ArchiveCompareResults.Hash_Not_Found_For_File)
                {

                    return comparisonResult;
                }

                // This code should never be reached
                msg = "Logic bug, CompRes = " + comparisonResult + " but should be ArchiveCompareResults.Hash_Not_Found_For_File";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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
        protected string ConvertServerPathToArchivePath(string datasetPathSvr, string datasetPathArch, string inpFileName)
        {
            string msg;

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

                msg = "Error in ConvertServerPathToArchivePath: File path '" + inpFileName + "' does not contain dataset path '" + datasetPathSvr + "'";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return PATH_CONVERSION_ERROR;

            }
            catch (Exception ex)
            {
                msg = "Exception converting server path to archive path for file " + datasetPathSvr + ": " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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
        protected ArchiveCompareResults CompareTwoFiles(string serverFile, string archiveFile, udtDatasetInfoType udtDatasetInfo)
        {
            string sFilePathInDictionary;

            var msg = "Comparing file " + serverFile + " to file " + archiveFile;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            // Get hash for archive file
            var archiveFileHash = GetArchiveFileHash(serverFile, udtDatasetInfo, out sFilePathInDictionary);
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "File '" + serverFile + "' has MyEMSL info in the MD5 results file, but was not found using the MyEMSLReader; this likely indicates a problem with Elastic Search");
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

            // Update the cached hash value to #HashMismatch#
            m_HashFileContents[sFilePathInDictionary].HashCode = HASH_MISMATCH;

            //Files not equal
            return ArchiveCompareResults.Compare_Not_Equal_or_Missing;

        }

        /// <summary>
        /// Delete the given folder if it is empty (no files, and all subfolders are empty)
        /// </summary>
        /// <param name="serverFolder"></param>
        /// <param name="iFoldersDeleted"></param>
        /// <returns>True if the folder was empty and was deleted; otherwise false</returns>
        protected bool DeleteFolderIfEmpty(string serverFolder, ref int iFoldersDeleted)
        {

            var diFolder = new DirectoryInfo(serverFolder);

            if (!diFolder.Exists)
            {
                return false;
            }

            if (diFolder.GetFiles("*.*", SearchOption.AllDirectories).Length == 0)
            {
#if DoDelete
                // This code will only be reached if conditional compilation symbol DoDelete is defined
                DeleteFolderRecurse(diFolder.FullName);
#endif
                iFoldersDeleted += 1;

                return true;
            }

            return false;
        }

        /// <summary>
        /// Deletes a folder, including all files and subfolders
        /// Assures that the ReadOnly bit is turned off for each folder
        /// </summary>
        /// <param name="sFolderPath"></param>
        /// <returns></returns>
        protected bool DeleteFolderRecurse(string sFolderPath)
        {
            var diFolder = new DirectoryInfo(sFolderPath);

            if (diFolder.Exists)
            {
                foreach (var diSubFolder in diFolder.GetDirectories())
                {
                    // Check whether the folder is marked as Read-Only
                    if ((diSubFolder.Attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                        diSubFolder.Attributes = diSubFolder.Attributes & ~FileAttributes.ReadOnly;

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


            // Folder not found; return true anyway
            return true;


        }

        /// <summary>
        /// Deletes all files in a folder, assuring that the ReadOnly bit is turned off for each file
        /// </summary>
        /// <param name="diFolder"></param>
        protected void DeleteFilesCheckReadonly(DirectoryInfo diFolder)
        {
            foreach (var fiFile in diFolder.GetFiles("*", SearchOption.AllDirectories))
            {
                if ((fiFile.Attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                    fiFile.Attributes = fiFile.Attributes & ~FileAttributes.ReadOnly;

                fiFile.Delete();
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
        protected string GenerateMD5ResultsFilePath(udtDatasetInfoType udtDatasetInfo)
        {
            var hashFileFolder = m_MgrParams.GetParam("MD5ResultsFolderPath");

            var sMD5ResultsFilePath = Path.Combine(hashFileFolder, udtDatasetInfo.Instrument);
            sMD5ResultsFilePath = Path.Combine(sMD5ResultsFilePath, udtDatasetInfo.YearQuarter);
            sMD5ResultsFilePath = Path.Combine(sMD5ResultsFilePath, RESULT_FILE_NAME_PREFIX + udtDatasetInfo.DatasetName);

            return sMD5ResultsFilePath;
        }

        /// <summary>
        /// Gets the hash value for a file from the results.datasetname file in the archive
        /// </summary>
        /// <param name="fileNamePath">File on storage server to find a matching archive hatch for</param>
        /// <param name="udtDatasetInfo">Dataset being purged</param>
        /// <param name="sFilePathInDictionary"></param>
        /// <returns>MD5 or Sha-1 Hash value for success; otherwise return (HASH) or an empty string</returns>
        protected string GetArchiveFileHash(string fileNamePath, udtDatasetInfoType udtDatasetInfo, out string sFilePathInDictionary)
        {
            // Archive should have a results.datasetname file for the purge candidate dataset. If present, the file
            // will have pre-calculated hash's for the files to be deleted. The manager will look for this result file,
            // and extract the file hash if found. If the hash file is not found, return an empty string,
            // telling the manager to request result file creation

            bool bHashFileLoaded;

            sFilePathInDictionary = string.Empty;

            var msg = "Getting archive hash for file " + fileNamePath;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            if (!string.IsNullOrEmpty(m_MD5ResultsFileDatasetName) && string.Equals(m_MD5ResultsFileDatasetName, udtDatasetInfo.DatasetName) && m_HashFileContents != null)
            {
                // Hash file has already been loaded into memory; no need to re-load it
                bHashFileLoaded = true;
            }
            else
            {
                bool bWaitingForMD5File;

                m_HashFileContents = new Dictionary<string, clsHashInfo>(StringComparer.CurrentCultureIgnoreCase);

                bHashFileLoaded = LoadMD5ResultsFile(udtDatasetInfo, out bWaitingForMD5File);

                if (!bHashFileLoaded)
                {
                    if (bWaitingForMD5File)
                        return WAITING_FOR_HASH_FILE;

                    // Error occurred (and has been logged)
                    return string.Empty;
                }
            }

            // Search the hash file contents for a file that matches the input file
            var filePathUnix = fileNamePath.Replace(@"\", @"/");

            var sSubfolderTofind = "/" + udtDatasetInfo.DatasetFolderName + "/";
            var sFileNameTrimmed = TrimPathAfterSubfolder(filePathUnix, sSubfolderTofind);

            if (string.IsNullOrEmpty(sFileNameTrimmed))
            {
                msg = "  Did not find " + sSubfolderTofind + " in path " + filePathUnix + " (original path " + fileNamePath + ")";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
            }
            else
            {
                clsHashInfo hashInfo;
                if (!m_HashFileContents.TryGetValue(sFileNameTrimmed, out hashInfo))
                {
                    msg = "  MD5 hash not found for file " + fileNamePath + " using " + sFileNameTrimmed + "; see results file " + m_MD5ResultsFilePath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    return HASH_NOT_FOUND;
                }

                sFilePathInDictionary = string.Copy(sFileNameTrimmed);
                return hashInfo.HashCode;
            }

            return string.Empty;

        }

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

            return PurgePolicyConstants.Auto;
        }

        /// <summary>
        /// Loads the MD5 results file for the given dataset into memory 
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="bWaitingForMD5File">Output parameter: true if an MD5 has file was not found</param>
        /// <returns></returns>
        protected bool LoadMD5ResultsFile(udtDatasetInfoType udtDatasetInfo, out bool bWaitingForMD5File)
        {

            string msg;

            // Find out if there's an MD5 results file for this dataset
            var sMD5ResultsFilePath = GenerateMD5ResultsFilePath(udtDatasetInfo);

            bWaitingForMD5File = false;
            m_MD5ResultsFileDatasetName = string.Empty;

            try
            {
                if (!File.Exists(sMD5ResultsFilePath))
                {
                    // MD5 results file not found
                    if (string.CompareOrdinal(udtDatasetInfo.DatasetName, m_LastMD5WarnDataset) != 0)
                    {
                        // Warning not yet posted
                        m_LastMD5WarnDataset = string.Copy(udtDatasetInfo.DatasetName);

                        msg = "  MD5 results file not found: " + sMD5ResultsFilePath;                        
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                        Console.WriteLine(msg);

                        // We stopped creating stage stagemd5 files in January 2016
                        // Thus, the following logic is now disabled

                        // Check to see if a stagemd5 file exists for this dataset. 
                        // This is for info only since this program does not create stagemd5 files (the DatasetPurgeArchiveHelper creates them)

                        //var hashFileFolder = m_MgrParams.GetParam("HashFileLocation");

                        //var stagedFileNamePath = Path.Combine(hashFileFolder, STAGED_FILE_NAME_PREFIX + udtDatasetInfo.DatasetName);
                        //if (File.Exists(stagedFileNamePath))
                        //{
                        //    msg = "  Found stagemd5 file: " + stagedFileNamePath;
                        //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                        //}
                        //else
                        //{
                        //    // DatasetPurgeArchiveHelper needs to create a stagemd5 file for this datatset
                        //    // Alternatively, if there are a bunch of stagemd5 files waiting to be processed,
                        //    //   eventually we should get MD5 result files and then we should be able to purge this dataset
                        //    msg = "  Stagemd5 file not found";
                        //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                        //}

                    }

                    bWaitingForMD5File = true;
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

                var sContents = File.ReadAllLines(sMD5ResultsFilePath);

                foreach (var sInputLine in sContents)
                {

                    // Extract the hash values value from the data line

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


                    var lstHashAndPathInfo = sInputLine.Split(new[] { ' ' }, 2).ToList();
                    if (lstHashAndPathInfo.Count > 1)
                    {

                        // For the above example, we want to store:
                        // "QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15.raw" and "0dcf9d677ac76519ae54c11cc5e10723" or
                        // "SIC201309041722_Auto976603/Default_2008-08-22.xml"    and "796d99bcc6f1824dfe1c36cc9a61636dd1b07625"

                        var hashCode = lstHashAndPathInfo[0];

                        var lstPathAndFileID = lstHashAndPathInfo[1].Split(new[] { '\t' }).ToList();

                        var sFileNamePath = lstPathAndFileID[0];

                        var myEmslFileID = string.Empty;
                        if (lstPathAndFileID.Count > 1)
                            myEmslFileID = lstPathAndFileID[1];

                        var sSubfolderTofind = "/" + udtDatasetInfo.DatasetFolderName + "/";

                        var sFileNameTrimmed = TrimPathAfterSubfolder(sFileNamePath, sSubfolderTofind);

                        if (string.IsNullOrEmpty(sFileNameTrimmed))
                        {
                            msg = "Did not find " + sSubfolderTofind + " in line " + sInputLine + " in results file " + sMD5ResultsFilePath;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                        }
                        else
                        {
                            var newHashInfo = new clsHashInfo(hashCode, myEmslFileID);

                            // Results files could have duplicate entries if a file was copied to the archive via FTP and was stored via MyEMSL
                            if (m_HashFileContents.ContainsKey(sFileNameTrimmed))
                            {
                                // Preferentially use the newer value, unless the older value is a MyEMSL Sha-1 hash but the newer value is an MD5 hash
                                if (!(hashCode.Length < 40 && m_HashFileContents[sFileNameTrimmed].HashCode.Length >= 40))
                                {

                                    m_HashFileContents[sFileNameTrimmed] = newHashInfo;
                                }
                            }
                            else
                                m_HashFileContents.Add(sFileNameTrimmed, newHashInfo);
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
        /// <param name="inpFileNamePath">Full path to file</param>
        /// <returns>String representation of hash</returns>
        protected string GenerateMD5HashFromFile(string inpFileNamePath)
        {
            string msg;
            byte[] byteHash;

            //Verify input file exists
            if (!File.Exists(inpFileNamePath))
            {
                msg = "clsUpdateOps.GenerateMD5HashFromFile; File not found: " + inpFileNamePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return "";
            }

            msg = "Generating MD5 hash for file " + inpFileNamePath;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            var hashTool = MD5.Create();

            var fi = new FileInfo(inpFileNamePath);
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
                msg = "clsUpdateOps.GenerateMD5HashFromFile; Exception generating hash for file " + inpFileNamePath + ": " + ex.Message;
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
            var hashStrBld = new StringBuilder();
            foreach (var t in byteHash)
            {
                hashStrBld.Append(t.ToString("x2"));
            }

            return hashStrBld.ToString();
        }

        protected string GenerateSha1HashFromFile(string inpFileNamePath)
        {
            string msg;

            //Verify input file exists
            if (!File.Exists(inpFileNamePath))
            {
                msg = "clsUpdateOps.GenerateSha1HashFromFile; File not found: " + inpFileNamePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return "";
            }

            msg = "Generating Sha-1 hash for file " + inpFileNamePath;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            var hashValue = Pacifica.Core.Utilities.GenerateSha1Hash(inpFileNamePath);

            return hashValue;
        }

        /// <summary>
        /// Call DMS to change AJ_Purged to 1 for the jobs in lstJobsToPurge
        /// </summary>
        /// <param name="lstJobsToPurge"></param>
        protected void MarkPurgedJobs(List<int> lstJobsToPurge)
        {
            const string SP_MARK_PURGED_JOBS = "MarkPurgedJobs";

            if (lstJobsToPurge.Count > 0)
            {
                // Construct a comma-separated list of jobs
                var sJobs = string.Empty;

                foreach (var job in lstJobsToPurge)
                {
                    if (sJobs.Length > 0)
                        sJobs += "," + job;
                    else
                        sJobs = job.ToString(CultureInfo.InvariantCulture);
                }

#if DoDelete
                // Call stored procedure MarkPurgedJobs

                var connStr = m_MgrParams.GetParam("ConnectionString");
                const int iMaxRetryCount = 3;
                string sErrorMessage;

                //Setup for execution of the stored procedure
                var MyCmd = new System.Data.SqlClient.SqlCommand();
                {
                    MyCmd.CommandType = System.Data.CommandType.StoredProcedure;
                    MyCmd.CommandText = SP_MARK_PURGED_JOBS;

                    var oParam = MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", System.Data.SqlDbType.Int));
                    oParam.Direction = System.Data.ParameterDirection.ReturnValue;

                    oParam = MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@JobList", System.Data.SqlDbType.VarChar, 4000));
                    oParam.Direction = System.Data.ParameterDirection.Input;
                    oParam.Value = sJobs;

                    oParam = MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@InfoOnly", System.Data.SqlDbType.TinyInt));
                    oParam.Direction = System.Data.ParameterDirection.Input;
                    oParam.Value = 0;
                }

                //Execute the SP
                var resCode = DMSProcedureExecutor.ExecuteSP(MyCmd, iMaxRetryCount, out sErrorMessage);
                string msg;
                if (resCode == 0)
                {
                    msg = "Marked job" + CheckPlural(lstJobsToPurge.Count) + " " + sJobs + " as purged";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    Console.WriteLine(msg);
                }
                else
                {
                    msg = "Error calling stored procedure " + SP_MARK_PURGED_JOBS + " to mark job" + CheckPlural(lstJobsToPurge.Count) + " " + sJobs + " as purged";
                    if (!string.IsNullOrEmpty(sErrorMessage))
                        msg += ": " + sErrorMessage;

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    Console.WriteLine(msg);
                }
#else
				var msg = "SIMULATE: call to " + SP_MARK_PURGED_JOBS + " for job" + CheckPlural(lstJobsToPurge.Count) + " " + sJobs;
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
        protected string TrimPathAfterSubfolder(string sFileNamePath, string sSubfolderTofind)
        {
            var iStartIndex = sFileNamePath.IndexOf(sSubfolderTofind, StringComparison.InvariantCultureIgnoreCase);

            if (iStartIndex < 0)
            {
                // Try again using lowercase
                iStartIndex = sFileNamePath.IndexOf(sSubfolderTofind, StringComparison.InvariantCultureIgnoreCase);
            }

            if (iStartIndex >= 0)
            {
                if (iStartIndex + sSubfolderTofind.Length < sFileNamePath.Length)
                    return sFileNamePath.Substring(iStartIndex + sSubfolderTofind.Length);

                return string.Empty;
            }

            return string.Empty;
        }

        /// <summary>
        /// Updates the archive hash file for a dataset to only retain lines where the MD5 hash value agree
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset Info</param>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        protected bool UpdateMD5ResultsFile(udtDatasetInfoType udtDatasetInfo)
        {
            string msg;
            var sCurrentStep = "Start";

            // Find out if there's a master MD5 results file for this dataset
            var sMD5ResultsFileMaster = GenerateMD5ResultsFilePath(udtDatasetInfo);

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
                var lstUpdatedMD5Info = new List<string>();
                var bWriteUpdatedMD5Info = false;

                var cSplitChars = new[] { ' ' };

                var sSubfolderTofind = "/" + udtDatasetInfo.DatasetFolderName + "/";

                sCurrentStep = "Read master MD5 results file";

                // Open the master MD5 results file and read each line
                using (var srMD5ResultsFileMaster = new StreamReader(new FileStream(sMD5ResultsFileMaster, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srMD5ResultsFileMaster.EndOfStream)
                    {
                        var sInputLine = srMD5ResultsFileMaster.ReadLine();
                        if (string.IsNullOrWhiteSpace(sInputLine))
                            continue;

                        // Extract the MD5 results value from sLineIn
                        // Format is MD5 code, then a space, then a full path to the file
                        // Example:
                        // 2036b65346acd59f3dd044b6a97bf44a /archive/dmsarch/LTQ_Orb_1/2008_1/EIF_Plasma_C_18_10Jan08_Draco_07-12-24/Seq200901221155_Auto362389/EIF_Plasma_C_18_10Jan08_Draco_07-12-24_out.zip

                        var lineParts = sInputLine.Split(cSplitChars, 2);
                        if (lineParts.Length <= 1)
                        {
                            continue;
                        }

                        // Look for the unix file path in m_HashFileContents
                        var sFileNameTrimmed = TrimPathAfterSubfolder(lineParts[1], sSubfolderTofind);

                        if (string.IsNullOrEmpty(sFileNameTrimmed))
                        {
                            // Did not find in lineParts[1]; this is unexpected
                            // An error should have already been logged when function LoadMD5ResultsFile() parsed this file
                            continue;
                        }

                        clsHashInfo hashInfo;
                        if (m_HashFileContents.TryGetValue(sFileNameTrimmed, out hashInfo))
                        {
                            // Match found; examine sMD5HashNew	
                            if (string.Equals(hashInfo.HashCode, HASH_MISMATCH))
                            {
                                // Old comment:
                                //   We need the DatasetPurgeArchiveHelper to create a new stagemd5 file that computes a new hash for this file
                                //   Do not include this line in lstUpdatedMD5Info;

                                // New Comment:
                                //   We need to run a new archive update job for this dataset
                                //   The ArchiveVerify tool will run as part of that job and will update the cached hash values

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

                    } // while not at EndOfStream

                }

                if (bWriteUpdatedMD5Info)
                {
                    var sMD5ResultsFilePathTemp = sMD5ResultsFileMaster + ".updated";

                    sCurrentStep = "Create " + sMD5ResultsFilePathTemp;
                    var swUpdatedMD5Results = new StreamWriter(new FileStream(sMD5ResultsFilePathTemp, FileMode.Create, FileAccess.Write, FileShare.Read));

                    foreach (var sOutputLine in lstUpdatedMD5Info)
                    {
                        swUpdatedMD5Results.WriteLine(sOutputLine);
                    }

                    swUpdatedMD5Results.Close();
                    System.Threading.Thread.Sleep(100);

                    sCurrentStep = "Overwrite master MD5 results file with " + sMD5ResultsFilePathTemp;
                    File.Copy(sMD5ResultsFilePathTemp, sMD5ResultsFileMaster, true);
                    System.Threading.Thread.Sleep(100);

                    sCurrentStep = "Delete " + sMD5ResultsFilePathTemp;
                    File.Delete(sMD5ResultsFilePathTemp);

                    msg = "  Updated MD5 results file " + sMD5ResultsFileMaster;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    Console.WriteLine(msg);

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
                msg = "Exception updating MD5 results file " + sMD5ResultsFileMaster + "; CurrentStep: " + sCurrentStep;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                return false;
            }
        }

        /// <summary>
        /// Validate that the share for the dataset actually exists
        /// </summary>
        /// <param name="sDatasetFolderPath"></param>
        /// <returns></returns>
        protected bool ValidateDatasetShareExists(string sDatasetFolderPath)
        {
            return ValidateDatasetShareExists(sDatasetFolderPath, -1);
        }

        /// <summary>
        /// Validate that the share for the dataset actually exists
        /// </summary>
        /// <param name="sDatasetFolderPath"></param>
        /// <param name="maxParentDepth">Maximum number of parent folders to examine when looking for a valid folder; -1 means parse all parent folders until a valid one is found</param>
        /// <returns>True if the dataset folder or the share that should have the dataset folder exists, other wise false</returns>
        protected bool ValidateDatasetShareExists(string sDatasetFolderPath, int maxParentDepth)
        {
            try
            {
                var diDatasetFolder = new DirectoryInfo(sDatasetFolderPath);

                if (diDatasetFolder.Exists)
                    return true;

                if (maxParentDepth == 0)
                    return false;

                var parentDepth = 0;

                while (diDatasetFolder.Parent != null)
                {
                    diDatasetFolder = diDatasetFolder.Parent;
                    if (diDatasetFolder.Exists)
                        return true;

                    parentDepth += 1;
                    if (maxParentDepth > -1 && parentDepth > maxParentDepth)
                        break;
                }

                return false;
            }
            catch (Exception ex)
            {
                var msg = "Exception validating that folder " + sDatasetFolderPath + " exists: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"
        void reader_ErrorEvent(object sender, MyEMSLReader.MessageEventArgs e)
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MyEMSLReader: " + e.Message);
        }

        void reader_MessageEvent(object sender, MyEMSLReader.MessageEventArgs e)
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message);
        }

        void reader_ProgressEvent(object sender, MyEMSLReader.ProgressEventArgs e)
        {
            var msg = "Percent complete: " + e.PercentComplete.ToString("0.0") + "%";

            /*
             * Logging of percent progress is disabled since we're only using the Reader to query for file information and not to download files from MyEMSL
             * 
            if (e.PercentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
            {
                if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    mPercentComplete = e.PercentComplete;
                    mLastProgressUpdateTime = DateTime.UtcNow;
                }
            }
            */
        }

        #endregion

    }
}
