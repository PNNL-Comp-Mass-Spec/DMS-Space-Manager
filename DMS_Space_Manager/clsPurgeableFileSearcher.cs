using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace Space_Manager
{
	class clsPurgeableFileSearcher
	{

		/// <summary>
		/// Look for files in diFolder matching sFilterSpec; do not recruse
		/// Matching files are added to lstServerFilesToPurge
		/// </summary>
		/// <param name="diFolder"></param>
		/// <param name="sFilterSpec"></param>
		/// <param name="lstServerFilesToPurge"></param>
		protected int AddFilesToPurge(DirectoryInfo diFolder, string sFilterSpec, ref SortedSet<string> lstServerFilesToPurge)
		{
			const int minSizeKB = 0;
			const bool recurse = false;
			return AddFilesToPurge(diFolder, sFilterSpec, minSizeKB, recurse, ref lstServerFilesToPurge);
		}

		/// <summary>
		/// Look for files in diFolder matching sFilterSpec, optionally recursing
		/// Filter by size if minSizeKB is greater than 0
		/// Matching files are added to lstServerFilesToPurge
		/// </summary>
		/// <param name="diFolder"></param>
		/// <param name="sFilterSpec"></param>
		/// <param name="minSizeKB"></param>
		/// <param name="recurse"></param>
		/// <param name="lstServerFilesToPurge"></param>
		/// <returns>The number of files added to lstServerFilesToPurge</returns>
		protected int AddFilesToPurge(DirectoryInfo diFolder, string sFilterSpec, int minSizeKB, bool recurse, ref SortedSet<string> lstServerFilesToPurge)
		{
			var iFilesMatched = 0;
			var sRequiredFileSuffix = string.Empty;

			SearchOption eSearchOption;
			if (recurse)
				eSearchOption = SearchOption.AllDirectories;
			else
				eSearchOption = SearchOption.TopDirectoryOnly;

			// If sFilterSpec is "*.baf" then .NET will match analysis.baf plus also analysis.baf_idx and analysis.baf_xtr
			// We instead want the beahvior to be like DOS, in that "*.baf" should only match analysis.baf
			if (sFilterSpec.StartsWith("*.") && char.IsLetterOrDigit(sFilterSpec[sFilterSpec.Length - 1]))
				sRequiredFileSuffix = sFilterSpec.Substring(1).ToLower();

			foreach (var fiFile in diFolder.GetFiles(sFilterSpec, eSearchOption))
			{
				if (sRequiredFileSuffix.Length == 0 || fiFile.Name.ToLower().EndsWith(sRequiredFileSuffix))
				{
					if (minSizeKB <= 0 || (fiFile.Length / 1024.0) >= minSizeKB)
					{
						if (!lstServerFilesToPurge.Contains(fiFile.FullName))
						{
							lstServerFilesToPurge.Add(fiFile.FullName);
						}

						iFilesMatched += 1;
					}
				}
			}

			return iFilesMatched;
		}

		/// <summary>
		/// Examines the file modification time of all files in diFolder
		/// If all are over iAgeThresholdDays old, then adds the files to lstServerFilesToPurge
		/// </summary>
		/// <param name="diFolder"></param>
		/// <param name="iAgeThresholdDays"></param>
		/// <param name="lstServerFilesToPurge"></param>
		/// <returns>True if the files were all older than the threshold, otherwise false</returns>
		protected bool AddFilesToPurgeDateThreshold(DirectoryInfo diFolder, int iAgeThresholdDays, ref SortedSet<string> lstServerFilesToPurge)
		{
			DateTime dtMostRecentUpdate;

			var lstFiles = FindFilesAndNewestDate(diFolder, out dtMostRecentUpdate);

			if (iAgeThresholdDays < 1)
				iAgeThresholdDays = 1;

			if (DateTime.UtcNow.Subtract(dtMostRecentUpdate).TotalDays > iAgeThresholdDays)
			{
				foreach (var sFile in lstFiles)
				{
					if (!lstServerFilesToPurge.Contains(sFile))
						lstServerFilesToPurge.Add(sFile);

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
		public SortedSet<string> FindDatasetFilesToPurge(DirectoryInfo diDatasetFolder, clsStorageOperations.udtDatasetInfoType udtDatasetInfo, out List<int> lstJobsToPurge)
		{
			SortedSet<string> lstServerFilesToPurge;

			var ePurgePolicyToApply = udtDatasetInfo.PurgePolicy;

			if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAll)
			{
				lstServerFilesToPurge = new SortedSet<string>();
				AddFilesToPurge(diDatasetFolder, "*.*", 0, true, ref lstServerFilesToPurge);

				lstJobsToPurge = new List<int>();
				return lstServerFilesToPurge;
			}

			if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAllExceptQC)
			{
				lstServerFilesToPurge = new SortedSet<string>();
				AddFilesToPurge(diDatasetFolder, "*.*", 0, false, ref lstServerFilesToPurge);

				foreach (var diSubFolder in diDatasetFolder.GetDirectories())
				{
					if (diSubFolder.Name != "QC")
						AddFilesToPurge(diSubFolder, "*.*", 0, true, ref lstServerFilesToPurge);
				}

				lstJobsToPurge = new List<int>();
				return lstServerFilesToPurge;
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

			var lstServerFilesToPurge = new SortedSet<string>();
			lstJobsToPurge = new List<int>();

			var reJobFolder = new Regex(@"_Auto(\d+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

			var diDatasetFolder = new DirectoryInfo(udtDatasetInfo.ServerFolderPath);

			// Process files in the dataset folder

			switch (udtDatasetInfo.RawDataType)
			{
				case "dot_raw_files":
					AddFilesToPurge(diDatasetFolder, "*.raw", ref lstServerFilesToPurge);
					break;

				case "dot_wiff_files":
				case "sciex_wiff_files":
					AddFilesToPurge(diDatasetFolder, "*.wiff", ref lstServerFilesToPurge);
					AddFilesToPurge(diDatasetFolder, "*.wiff.scan", ref lstServerFilesToPurge);
					break;

				case "dot_mzml_files":
					AddFilesToPurge(diDatasetFolder, "*.mzML", ref lstServerFilesToPurge);
					AddFilesToPurge(diDatasetFolder, "*.mzXML", ref lstServerFilesToPurge);
					break;

				case "dot_uimf_files":
					AddFilesToPurge(diDatasetFolder, "*.uimf", ref lstServerFilesToPurge);
					break;

				case "bruker_maldi_imaging":
					AddFilesToPurge(diDatasetFolder, "*.zip", ref lstServerFilesToPurge);
					break;

				case "zipped_s_folders":
					AddFilesToPurge(diDatasetFolder, "*.zip", ref lstServerFilesToPurge);
					break;

				case "bruker_maldi_spot":
					AddFilesToPurge(diDatasetFolder, "*.*", ref lstServerFilesToPurge);
					break;
			}

			// Purge all files over 2 MB in size
			AddFilesToPurge(diDatasetFolder, "*.*", 2048, false, ref lstServerFilesToPurge);

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
					AddFilesToPurge(diSubDir, "*.yep", 0, true, ref lstServerFilesToPurge);				// bruker_ft and bruker_tof_baf
					AddFilesToPurge(diSubDir, "*.baf", 0, true, ref lstServerFilesToPurge);				// bruker_ft and bruker_tof_baf
					AddFilesToPurge(diSubDir, "ser", 0, true, ref lstServerFilesToPurge);				// bruker_ft and bruker_tof_baf
					AddFilesToPurge(diSubDir, "fid", 0, true, ref lstServerFilesToPurge);				// bruker_ft and bruker_tof_baf
					AddFilesToPurge(diSubDir, "DATA.MS", 0, true, ref lstServerFilesToPurge);			// Agilent_GC_MS_01
					AddFilesToPurge(diSubDir, "MSProfile.bin", 0, true, ref lstServerFilesToPurge);		// Agilent_QTof

					// Purge all files over 2 MB in size
					AddFilesToPurge(diSubDir, "*.*", 2048, true, ref lstServerFilesToPurge);
					continue;
				}

				if (subDirNameUpper.EndsWith(".RAW"))
				{
					AddFilesToPurge(diSubDir, "*.raw", 0, true, ref lstServerFilesToPurge);

					// Purge all files over 2 MB in size
					AddFilesToPurge(diSubDir, "*.*", 2048, true, ref lstServerFilesToPurge);
					continue;
				}

				if (subDirNameUpper.StartsWith("MSXML_GEN"))
				{
					AddFilesToPurgeDateThreshold(diSubDir, 90, ref lstServerFilesToPurge);
					continue;
				}

				if (subDirNameUpper.StartsWith("DTA_GEN") || subDirNameUpper.StartsWith("DTA_REF"))
				{
					// Purge after 1.5 years
					AddFilesToPurgeDateThreshold(diSubDir, 548, ref lstServerFilesToPurge);
					continue;
				}
				
				var reMatch = reJobFolder.Match(diSubDir.Name);
				if (reMatch.Success)
				{
					// This is an analysis job folder
					if (diSubDir.Name.StartsWith("SIC"))
					{
						// This is a MASIC folder
						AddFilesToPurge(diSubDir, "*.zip", ref lstServerFilesToPurge);

						// Purge all files over 15 MB in size
						AddFilesToPurge(diSubDir, "*.*", 15 * 1024, true, ref lstServerFilesToPurge);
					}
					else
					{
						// Other analysis job folders
						// Purge the entire folder if all files are over 3 years old
						var bSubDirPurged = AddFilesToPurgeDateThreshold(diSubDir, 3 * 365, ref lstServerFilesToPurge);

						if (!bSubDirPurged)
						{
							// Files are not yet 3 years old
							// If all of the files are 1 year old, then purge files over 50 MB

							DateTime dtMostRecentUpdate;

							var lstFiles = FindFilesAndNewestDate(diSubDir, out dtMostRecentUpdate);

							if (DateTime.UtcNow.Subtract(dtMostRecentUpdate).TotalDays > 365)
							{
								// Purge all files over 50 MB in size
								var iFilesMatched = AddFilesToPurge(diSubDir, "*.*", 50 * 1024, true, ref lstServerFilesToPurge);

								if (iFilesMatched == lstFiles.Count)
									bSubDirPurged = true;
							}
						}

						if (bSubDirPurged)
						{
							if (reMatch.Groups.Count > 0)
							{
								int jobNum;
								if (int.TryParse(reMatch.Groups[1].Value, out jobNum))
									lstJobsToPurge.Add(jobNum);
							}
						}

					}

					continue;
				}

				// Use a threshold of 9 months for all other subfolders
				AddFilesToPurgeDateThreshold(diSubDir, 270, ref lstServerFilesToPurge);

			}

			return lstServerFilesToPurge;

		}

	}
}
