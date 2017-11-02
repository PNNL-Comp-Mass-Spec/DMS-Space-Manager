using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace Space_Manager
{
    class clsPurgeableFileSearcher
    {
        /// <summary>
        /// Look for files in diFolder matching filterSpec; do not recruse
        /// Matching files are added to serverFilesToPurge
        /// </summary>
        /// <param name="diFolder"></param>
        /// <param name="filterSpec"></param>
        /// <param name="serverFilesToPurge"></param>
        private void AddFilesToPurge(DirectoryInfo diFolder, string filterSpec, ISet<string> serverFilesToPurge)
        {
            const int minSizeKB = 0;
            const bool recurse = false;
            AddFilesToPurge(diFolder, filterSpec, minSizeKB, recurse, serverFilesToPurge);
        }

        /// <summary>
        /// Look for files in diFolder matching filterSpec, optionally recursing
        /// Filter by size if minSizeKB is greater than 0
        /// Matching files are added to serverFilesToPurge
        /// </summary>
        /// <param name="diFolder"></param>
        /// <param name="filterSpec"></param>
        /// <param name="minSizeKB"></param>
        /// <param name="recurse"></param>
        /// <param name="serverFilesToPurge"></param>
        /// <returns>The number of files added to serverFilesToPurge</returns>
        private int AddFilesToPurge(DirectoryInfo diFolder, string filterSpec, int minSizeKB, bool recurse, ISet<string> serverFilesToPurge)
        {
            var filesMatched = 0;
            var requiredFileSuffix = string.Empty;

            SearchOption eSearchOption;
            if (recurse)
                eSearchOption = SearchOption.AllDirectories;
            else
                eSearchOption = SearchOption.TopDirectoryOnly;

            // If filterSpec is "*.baf" then .NET will match analysis.baf plus also analysis.baf_idx and analysis.baf_xtr
            // We instead want the beahvior to be like DOS, in that "*.baf" should only match analysis.baf
            if (filterSpec.StartsWith("*.") && char.IsLetterOrDigit(filterSpec[filterSpec.Length - 1]))
                requiredFileSuffix = filterSpec.Substring(1);

            foreach (var fiFile in diFolder.GetFiles(filterSpec, eSearchOption))
            {
                if (requiredFileSuffix.Length == 0 || fiFile.Name.EndsWith(requiredFileSuffix, StringComparison.InvariantCultureIgnoreCase))
                {
                    if (minSizeKB <= 0 || (fiFile.Length / 1024.0) >= minSizeKB)
                    {
                        if (!serverFilesToPurge.Contains(fiFile.FullName))
                        {
                            serverFilesToPurge.Add(fiFile.FullName);
                        }

                        filesMatched += 1;
                    }
                }
            }

            return filesMatched;
        }

        /// <summary>
        /// Examines the file modification time of all files in diFolder
        /// If all are over ageThresholdDays old, then adds the files to serverFilesToPurge
        /// </summary>
        /// <param name="diFolder"></param>
        /// <param name="ageThresholdDays"></param>
        /// <param name="serverFilesToPurge"></param>
        /// <returns>True if the files were all older than the threshold, otherwise false</returns>
        private bool AddFilesToPurgeDateThreshold(DirectoryInfo diFolder, int ageThresholdDays, ISet<string> serverFilesToPurge)
        {

            var lstFiles = FindFilesAndNewestDate(diFolder, out var dtMostRecentUpdate);

            if (ageThresholdDays < 1)
                ageThresholdDays = 1;

            if (DateTime.UtcNow.Subtract(dtMostRecentUpdate).TotalDays > ageThresholdDays)
            {
                foreach (var file in lstFiles)
                {
                    if (!serverFilesToPurge.Contains(file))
                        serverFilesToPurge.Add(file);

                }
                return true;
            }

            return false;

        }

        /// <summary>
        /// Examine each file in serverFiles and decide which is safe to delete based on the purge policy
        /// </summary>
        /// <param name="diDatasetFolder">Dataset folder to process</param>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="lstJobsToPurge">Jobs whose folders will be deleted</param>
        /// <returns>List of files that are safe to delete</returns>
        public SortedSet<string> FindDatasetFilesToPurge(
            DirectoryInfo diDatasetFolder,
            clsStorageOperations.udtDatasetInfoType udtDatasetInfo,
            out List<int> lstJobsToPurge)
        {

            var ePurgePolicyToApply = udtDatasetInfo.PurgePolicy;

            if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAll)
            {
                var serverFilesToPurge = new SortedSet<string>();
                AddFilesToPurge(diDatasetFolder, "*.*", 0, true, serverFilesToPurge);

                lstJobsToPurge = new List<int>();
                return serverFilesToPurge;
            }

            if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAllExceptQC)
            {
                var serverFilesToPurge = new SortedSet<string>();
                AddFilesToPurge(diDatasetFolder, "*.*", 0, false, serverFilesToPurge);

                foreach (var diSubFolder in diDatasetFolder.GetDirectories())
                {
                    if (diSubFolder.Name != "QC")
                        AddFilesToPurge(diSubFolder, "*.*", 0, true, serverFilesToPurge);
                }

                lstJobsToPurge = new List<int>();
                return serverFilesToPurge;
            }

            // Auto-purge files for this dataset
            return FindDatasetFilesToAutoPurge(udtDatasetInfo, out lstJobsToPurge);

        }

        private List<string> FindFilesAndNewestDate(DirectoryInfo diFolder, out DateTime dtMostRecentUpdate)
        {
            var lstFiles = new List<string>();
            dtMostRecentUpdate = DateTime.MinValue;

            // Find files in diFolder
            foreach (var fiFile in diFolder.GetFiles("*.*", SearchOption.AllDirectories))
            {
                if (fiFile.LastWriteTimeUtc > dtMostRecentUpdate)
                {
                    dtMostRecentUpdate = fiFile.LastWriteTimeUtc;
                }

                lstFiles.Add(fiFile.FullName);
            }

            return lstFiles;

        }

        /// <summary>
        /// Examine the files that exist below a dataset folder
        /// Auto-determine the ones that should be purged
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="lstJobsToPurge">Jobs whose folders will be deleted</param>
        /// <returns>List of files that are safe to delete</returns>
        public SortedSet<string> FindDatasetFilesToAutoPurge(clsStorageOperations.udtDatasetInfoType udtDatasetInfo, out List<int> lstJobsToPurge)
        {

            var serverFilesToPurge = new SortedSet<string>();
            lstJobsToPurge = new List<int>();

            var reJobFolder = new Regex(@"_Auto(\d+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var diDatasetFolder = new DirectoryInfo(udtDatasetInfo.ServerFolderPath);

            // Process files in the dataset folder

            switch (udtDatasetInfo.RawDataType)
            {
                case "dot_raw_files":
                    AddFilesToPurge(diDatasetFolder, "*.raw", serverFilesToPurge);
                    break;

                case "dot_wiff_files":
                case "sciex_wiff_files":
                    AddFilesToPurge(diDatasetFolder, "*.wiff", serverFilesToPurge);
                    AddFilesToPurge(diDatasetFolder, "*.wiff.scan", serverFilesToPurge);
                    break;

                case "dot_mzml_files":
                    AddFilesToPurge(diDatasetFolder, "*.mzML", serverFilesToPurge);
                    AddFilesToPurge(diDatasetFolder, "*.mzXML", serverFilesToPurge);
                    break;

                case "dot_uimf_files":
                    AddFilesToPurge(diDatasetFolder, "*.uimf", serverFilesToPurge);
                    break;

                case "bruker_maldi_imaging":
                    AddFilesToPurge(diDatasetFolder, "*.zip", serverFilesToPurge);
                    break;

                case "zipped_s_folders":
                    AddFilesToPurge(diDatasetFolder, "*.zip", serverFilesToPurge);
                    break;

                case "bruker_maldi_spot":
                    AddFilesToPurge(diDatasetFolder, "*.*", serverFilesToPurge);
                    break;
            }

            // Purge all files over 2 MB in size
            AddFilesToPurge(diDatasetFolder, "*.*", 2048, false, serverFilesToPurge);

            // Process the directories below the dataset folder

            // Construct a list of the folders that exist at the dataset folder level
            foreach (var diSubDir in diDatasetFolder.GetDirectories())
            {
                var subDirNameUpper = diSubDir.Name.ToUpper();

                if (diSubDir.Name == "QC")
                    // Do not purge the QC folder
                    continue;

                if (subDirNameUpper.EndsWith(".D"))
                {
                    // Instrument data folder
                    AddFilesToPurge(diSubDir, "*.yep", 0, true, serverFilesToPurge);				// bruker_ft and bruker_tof_baf
                    AddFilesToPurge(diSubDir, "*.baf", 0, true, serverFilesToPurge);				// bruker_ft and bruker_tof_baf
                    AddFilesToPurge(diSubDir, "ser", 0, true, serverFilesToPurge);				    // bruker_ft and bruker_tof_baf
                    AddFilesToPurge(diSubDir, "fid", 0, true, serverFilesToPurge);				    // bruker_ft and bruker_tof_baf
                    AddFilesToPurge(diSubDir, "DATA.MS", 0, true, serverFilesToPurge);			    // Agilent_GC_MS_01
                    AddFilesToPurge(diSubDir, "MSProfile.bin", 0, true, serverFilesToPurge);		// Agilent_QTof
                    AddFilesToPurge(diSubDir, "MSPeak.bin", 250, true, serverFilesToPurge);		    // Agilent_QQQ
                    AddFilesToPurge(diSubDir, "MSScan.bin", 1000, true, serverFilesToPurge);		// Agilent_QQQ

                    // Purge all files over 2 MB in size
                    AddFilesToPurge(diSubDir, "*.*", 2048, true, serverFilesToPurge);
                    continue;
                }

                if (subDirNameUpper.EndsWith(".RAW"))
                {
                    AddFilesToPurge(diSubDir, "*.raw", 0, true, serverFilesToPurge);

                    // Purge all files over 2 MB in size
                    AddFilesToPurge(diSubDir, "*.*", 2048, true, serverFilesToPurge);
                    continue;
                }

                if (subDirNameUpper.StartsWith("MSXML_GEN"))
                {
                    AddFilesToPurgeDateThreshold(diSubDir, 90, serverFilesToPurge);
                    continue;
                }

                if (subDirNameUpper.StartsWith("DTA_GEN") || subDirNameUpper.StartsWith("DTA_REF"))
                {
                    // Purge after 1.5 years
                    AddFilesToPurgeDateThreshold(diSubDir, 548, serverFilesToPurge);
                    continue;
                }

                var reMatch = reJobFolder.Match(diSubDir.Name);
                if (reMatch.Success)
                {
                    // This is an analysis job folder
                    if (diSubDir.Name.StartsWith("SIC"))
                    {
                        // This is a MASIC folder
                        AddFilesToPurge(diSubDir, "*.zip", serverFilesToPurge);

                        // Purge all files over 15 MB in size
                        AddFilesToPurge(diSubDir, "*.*", 15 * 1024, true, serverFilesToPurge);
                    }
                    else
                    {
                        // Other analysis job folders
                        // Purge the entire folder if all files are over 3 years old
                        var subDirPurged = AddFilesToPurgeDateThreshold(diSubDir, 3 * 365, serverFilesToPurge);

                        if (!subDirPurged)
                        {
                            // Files are not yet 3 years old
                            // If all of the files are 1 year old, then purge files over 50 MB


                            var lstFiles = FindFilesAndNewestDate(diSubDir, out var dtMostRecentUpdate);

                            if (DateTime.UtcNow.Subtract(dtMostRecentUpdate).TotalDays > 365)
                            {
                                // Purge all files over 50 MB in size
                                var filesMatched = AddFilesToPurge(diSubDir, "*.*", 50 * 1024, true, serverFilesToPurge);

                                if (filesMatched == lstFiles.Count)
                                    subDirPurged = true;
                            }
                        }

                        if (subDirPurged)
                        {
                            if (reMatch.Groups.Count > 0)
                            {
                                if (int.TryParse(reMatch.Groups[1].Value, out var jobNum))
                                    lstJobsToPurge.Add(jobNum);
                            }
                        }

                    }

                    continue;
                }

                // Use a threshold of 9 months for all other subfolders
                AddFilesToPurgeDateThreshold(diSubDir, 270, serverFilesToPurge);

            }

            return serverFilesToPurge;

        }

    }
}
