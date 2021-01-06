﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace Space_Manager
{
    class clsPurgeableFileSearcher
    {
        /// <summary>
        /// Look for files in targetDirectory matching filterSpec; do not recurse
        /// Matching files are added to serverFilesToPurge
        /// </summary>
        /// <param name="targetDirectory"></param>
        /// <param name="filterSpec"></param>
        /// <param name="serverFilesToPurge"></param>
        private void AddFilesToPurge(DirectoryInfo targetDirectory, string filterSpec, ISet<string> serverFilesToPurge)
        {
            const int minSizeKB = 0;
            const bool recurse = false;
            AddFilesToPurge(targetDirectory, filterSpec, minSizeKB, recurse, serverFilesToPurge);
        }

        /// <summary>
        /// Look for files in targetDirectory matching filterSpec, optionally recursing
        /// Filter by size if minSizeKB is greater than 0
        /// Matching files are added to serverFilesToPurge
        /// </summary>
        /// <param name="targetDirectory"></param>
        /// <param name="filterSpec"></param>
        /// <param name="minSizeKB"></param>
        /// <param name="recurse"></param>
        /// <param name="serverFilesToPurge"></param>
        /// <returns>The number of files added to serverFilesToPurge</returns>
        private int AddFilesToPurge(DirectoryInfo targetDirectory, string filterSpec, int minSizeKB, bool recurse, ISet<string> serverFilesToPurge)
        {
            var filesMatched = 0;
            var requiredFileSuffix = string.Empty;

            SearchOption eSearchOption;
            if (recurse)
                eSearchOption = SearchOption.AllDirectories;
            else
                eSearchOption = SearchOption.TopDirectoryOnly;

            // If filterSpec is "*.baf" then .NET will match analysis.baf plus also analysis.baf_idx and analysis.baf_xtr
            // We instead want the behavior to be like DOS, in that "*.baf" should only match analysis.baf
            if (filterSpec.StartsWith("*.") && char.IsLetterOrDigit(filterSpec[filterSpec.Length - 1]))
                requiredFileSuffix = filterSpec.Substring(1);

            foreach (var candidateFile in targetDirectory.GetFiles(filterSpec, eSearchOption))
            {
                if (requiredFileSuffix.Length == 0 || candidateFile.Name.EndsWith(requiredFileSuffix, StringComparison.InvariantCultureIgnoreCase))
                {
                    if (minSizeKB <= 0 || (candidateFile.Length / 1024.0) >= minSizeKB)
                    {
                        if (!serverFilesToPurge.Contains(candidateFile.FullName))
                        {
                            serverFilesToPurge.Add(candidateFile.FullName);
                        }

                        filesMatched += 1;
                    }
                }
            }

            return filesMatched;
        }

        /// <summary>
        /// Examines the file modification time of all files in targetDirectory
        /// If all are over ageThresholdDays old, then adds the files to serverFilesToPurge
        /// </summary>
        /// <param name="targetDirectory"></param>
        /// <param name="ageThresholdDays"></param>
        /// <param name="serverFilesToPurge"></param>
        /// <returns>True if the files were all older than the threshold, otherwise false</returns>
        private bool AddFilesToPurgeDateThreshold(DirectoryInfo targetDirectory, int ageThresholdDays, ISet<string> serverFilesToPurge)
        {

            var lstFiles = FindFilesAndNewestDate(targetDirectory, out var dtMostRecentUpdate);

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
        /// <param name="datasetDirectory">Dataset directory to process</param>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="lstJobsToPurge">Jobs whose directories will be deleted</param>
        /// <returns>List of files that are safe to delete</returns>
        public SortedSet<string> FindDatasetFilesToPurge(
            DirectoryInfo datasetDirectory,
            clsStorageOperations.udtDatasetInfoType udtDatasetInfo,
            out List<int> lstJobsToPurge)
        {

            var ePurgePolicyToApply = udtDatasetInfo.PurgePolicy;

            if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAll)
            {
                var serverFilesToPurge = new SortedSet<string>();
                AddFilesToPurge(datasetDirectory, "*.*", 0, true, serverFilesToPurge);

                lstJobsToPurge = new List<int>();
                return serverFilesToPurge;
            }

            if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAllExceptQC)
            {
                var serverFilesToPurge = new SortedSet<string>();
                AddFilesToPurge(datasetDirectory, "*.*", 0, false, serverFilesToPurge);

                foreach (var subdirectory in datasetDirectory.GetDirectories())
                {
                    if (subdirectory.Name != "QC")
                        AddFilesToPurge(subdirectory, "*.*", 0, true, serverFilesToPurge);
                }

                lstJobsToPurge = new List<int>();
                return serverFilesToPurge;
            }

            // Auto-purge files for this dataset
            return FindDatasetFilesToAutoPurge(udtDatasetInfo, out lstJobsToPurge);

        }

        private List<string> FindFilesAndNewestDate(DirectoryInfo targetDirectory, out DateTime dtMostRecentUpdate)
        {
            var lstFiles = new List<string>();
            dtMostRecentUpdate = DateTime.MinValue;

            // Find files in targetDirectory
            foreach (var candidateFile in targetDirectory.GetFiles("*.*", SearchOption.AllDirectories))
            {
                if (candidateFile.LastWriteTimeUtc > dtMostRecentUpdate)
                {
                    dtMostRecentUpdate = candidateFile.LastWriteTimeUtc;
                }

                lstFiles.Add(candidateFile.FullName);
            }

            return lstFiles;

        }

        /// <summary>
        /// Examine the files that exist below a dataset directory
        /// Auto-determine the ones that should be purged
        /// </summary>
        /// <param name="udtDatasetInfo">Dataset info</param>
        /// <param name="lstJobsToPurge">Jobs whose directories will be deleted</param>
        /// <returns>List of files that are safe to delete</returns>
        public SortedSet<string> FindDatasetFilesToAutoPurge(clsStorageOperations.udtDatasetInfoType udtDatasetInfo, out List<int> lstJobsToPurge)
        {

            var serverFilesToPurge = new SortedSet<string>();
            lstJobsToPurge = new List<int>();

            var jobDirectoryMatcher = new Regex(@"_Auto(\d+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var datasetDirectory = new DirectoryInfo(udtDatasetInfo.DatasetDirectoryPath);

            // Process files in the dataset directory

            switch (udtDatasetInfo.RawDataType)
            {
                case "dot_raw_files":
                    AddFilesToPurge(datasetDirectory, "*.raw", serverFilesToPurge);
                    break;

                case "dot_wiff_files":
                case "sciex_wiff_files":
                    AddFilesToPurge(datasetDirectory, "*.wiff", serverFilesToPurge);
                    AddFilesToPurge(datasetDirectory, "*.wiff.scan", serverFilesToPurge);
                    break;

                case "dot_mzml_files":
                    AddFilesToPurge(datasetDirectory, "*.mzML", serverFilesToPurge);
                    AddFilesToPurge(datasetDirectory, "*.mzXML", serverFilesToPurge);
                    break;

                case "dot_uimf_files":
                    AddFilesToPurge(datasetDirectory, "*.uimf", serverFilesToPurge);
                    break;

                case "bruker_maldi_imaging":
                    AddFilesToPurge(datasetDirectory, "*.zip", serverFilesToPurge);
                    break;

                case "zipped_s_folders":
                    AddFilesToPurge(datasetDirectory, "*.zip", serverFilesToPurge);
                    break;

                case "bruker_maldi_spot":
                    AddFilesToPurge(datasetDirectory, "*.*", serverFilesToPurge);
                    break;
            }

            // Purge all files over 2 MB in size
            AddFilesToPurge(datasetDirectory, "*.*", 2048, false, serverFilesToPurge);

            // Process the directories below the dataset directory

            // Construct a list of the directories that exist at the dataset directory level
            foreach (var subdirectory in datasetDirectory.GetDirectories())
            {
                var subDirNameUpper = subdirectory.Name.ToUpper();

                if (subdirectory.Name == "QC")
                    // Do not purge the QC directory
                    continue;

                if (subDirNameUpper.EndsWith(".D"))
                {
                    // Instrument data directory
                    AddFilesToPurge(subdirectory, "*.yep", 0, true, serverFilesToPurge);				// bruker_ft and bruker_tof_baf
                    AddFilesToPurge(subdirectory, "*.baf", 0, true, serverFilesToPurge);				// bruker_ft and bruker_tof_baf
                    AddFilesToPurge(subdirectory, "ser", 0, true, serverFilesToPurge);				    // bruker_ft and bruker_tof_baf
                    AddFilesToPurge(subdirectory, "fid", 0, true, serverFilesToPurge);				    // bruker_ft and bruker_tof_baf
                    AddFilesToPurge(subdirectory, "DATA.MS", 0, true, serverFilesToPurge);			    // Agilent_GC_MS_01
                    AddFilesToPurge(subdirectory, "MSProfile.bin", 0, true, serverFilesToPurge);		// Agilent_QTof
                    AddFilesToPurge(subdirectory, "MSPeak.bin", 250, true, serverFilesToPurge);		    // Agilent_QQQ
                    AddFilesToPurge(subdirectory, "MSScan.bin", 1000, true, serverFilesToPurge);		// Agilent_QQQ

                    // Purge all files over 2 MB in size
                    AddFilesToPurge(subdirectory, "*.*", 2048, true, serverFilesToPurge);
                    continue;
                }

                if (subDirNameUpper.EndsWith(".RAW"))
                {
                    AddFilesToPurge(subdirectory, "*.raw", 0, true, serverFilesToPurge);

                    // Purge all files over 2 MB in size
                    AddFilesToPurge(subdirectory, "*.*", 2048, true, serverFilesToPurge);
                    continue;
                }

                if (subDirNameUpper.StartsWith("MSXML_GEN"))
                {
                    AddFilesToPurgeDateThreshold(subdirectory, 90, serverFilesToPurge);
                    continue;
                }

                if (subDirNameUpper.StartsWith("DTA_GEN") || subDirNameUpper.StartsWith("DTA_REF"))
                {
                    // Purge after 1.5 years
                    AddFilesToPurgeDateThreshold(subdirectory, 548, serverFilesToPurge);
                    continue;
                }

                var match = jobDirectoryMatcher.Match(subdirectory.Name);
                if (match.Success)
                {
                    // This is an analysis job directory
                    if (subdirectory.Name.StartsWith("SIC"))
                    {
                        // This is a MASIC directory
                        AddFilesToPurge(subdirectory, "*.zip", serverFilesToPurge);

                        // Purge all files over 15 MB in size
                        AddFilesToPurge(subdirectory, "*.*", 15 * 1024, true, serverFilesToPurge);
                    }
                    else
                    {
                        // Other analysis job directories
                        // Purge the entire directory if all files are over 3 years old
                        var subDirPurged = AddFilesToPurgeDateThreshold(subdirectory, 3 * 365, serverFilesToPurge);

                        if (!subDirPurged)
                        {
                            // Files are not yet 3 years old
                            // If all of the files are 1 year old, then purge files over 50 MB


                            var lstFiles = FindFilesAndNewestDate(subdirectory, out var dtMostRecentUpdate);

                            if (DateTime.UtcNow.Subtract(dtMostRecentUpdate).TotalDays > 365)
                            {
                                // Purge all files over 50 MB in size
                                var filesMatched = AddFilesToPurge(subdirectory, "*.*", 50 * 1024, true, serverFilesToPurge);

                                if (filesMatched == lstFiles.Count)
                                    subDirPurged = true;
                            }
                        }

                        if (subDirPurged)
                        {
                            if (match.Groups.Count > 0)
                            {
                                if (int.TryParse(match.Groups[1].Value, out var jobNum))
                                    lstJobsToPurge.Add(jobNum);
                            }
                        }

                    }

                    continue;
                }

                // Use a threshold of 9 months for all other subdirectories
                AddFilesToPurgeDateThreshold(subdirectory, 270, serverFilesToPurge);

            }

            return serverFilesToPurge;

        }

    }
}
