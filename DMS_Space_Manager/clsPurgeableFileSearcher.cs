using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
		protected int AddFilesToPurge(System.IO.DirectoryInfo diFolder, string sFilterSpec, ref System.Collections.Generic.SortedSet<string> lstServerFilesToPurge)
		{
			int minSizeKB = 0;
			bool recurse = false;
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
		protected int AddFilesToPurge(System.IO.DirectoryInfo diFolder, string sFilterSpec, int minSizeKB, bool recurse, ref System.Collections.Generic.SortedSet<string> lstServerFilesToPurge)
		{
			int iFilesMatched = 0;
			string sRequiredFileSuffix = string.Empty;

			System.IO.SearchOption eSearchOption;
			if (recurse)
				eSearchOption = System.IO.SearchOption.AllDirectories;
			else
				eSearchOption = System.IO.SearchOption.TopDirectoryOnly;

			// If sFilterSpec is "*.baf" then .NET will match analysis.baf plus also analysis.baf_idx and analysis.baf_xtr
			// We instead want the beahvior to be like DOS, in that "*.baf" should only match analysis.baf
			if (sFilterSpec.StartsWith("*.") && char.IsLetterOrDigit(sFilterSpec[sFilterSpec.Length - 1]))
				sRequiredFileSuffix = sFilterSpec.Substring(1).ToLower();

			foreach (System.IO.FileInfo fiFile in diFolder.GetFiles(sFilterSpec, eSearchOption))
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
		protected bool AddFilesToPurgeDateThreshold(System.IO.DirectoryInfo diFolder, int iAgeThresholdDays, ref System.Collections.Generic.SortedSet<string> lstServerFilesToPurge)
		{
			System.Collections.Generic.List<string> lstFiles;
			System.DateTime dtMostRecentUpdate;

			lstFiles = FindFilesAndNewestDate(diFolder, out dtMostRecentUpdate);

			if (iAgeThresholdDays < 1)
				iAgeThresholdDays = 1;

			if (System.DateTime.UtcNow.Subtract(dtMostRecentUpdate).TotalDays > iAgeThresholdDays)
			{
				foreach (string sFile in lstFiles)
				{
					if (!lstServerFilesToPurge.Contains(sFile))
						lstServerFilesToPurge.Add(sFile);

				}
				return true;
			}
			else
				return false;

		}


		/// <summary>
		/// Examine each file in serverFiles and decide which is safe to delete based on the purge policy
		/// </summary>
		/// <param name="serverFiles">Files that were found for this dataset on the server</param>
		/// <param name="udtDatasetInfo">Dataset info</param>
		/// <param name="lstJobsToPurge">Jobs whose folders will be deleted</param>
		/// <returns>List of files that are safe to delete</returns>
		public System.Collections.Generic.SortedSet<string> FindDatasetFilesToPurge(System.IO.DirectoryInfo diDatasetFolder, clsStorageOperations.udtDatasetInfoType udtDatasetInfo, out System.Collections.Generic.List<int> lstJobsToPurge)
		{
			System.Collections.Generic.SortedSet<string> lstServerFilesToPurge;

			clsStorageOperations.PurgePolicyConstants ePurgePolicyToApply = udtDatasetInfo.PurgePolicy;

			if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAll)
			{
				lstServerFilesToPurge = new System.Collections.Generic.SortedSet<string>();
				AddFilesToPurge(diDatasetFolder, "*.*", 0, true, ref lstServerFilesToPurge);

				lstJobsToPurge = new List<int>();
				return lstServerFilesToPurge;
			}

			if (ePurgePolicyToApply == clsStorageOperations.PurgePolicyConstants.PurgeAllExceptQC)
			{
				lstServerFilesToPurge = new System.Collections.Generic.SortedSet<string>();
				AddFilesToPurge(diDatasetFolder, "*.*", 0, false, ref lstServerFilesToPurge);

				foreach (System.IO.DirectoryInfo diSubFolder in diDatasetFolder.GetDirectories())
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

		private System.Collections.Generic.List<string> FindFilesAndNewestDate(System.IO.DirectoryInfo diFolder, out System.DateTime dtMostRecentUpdate)
		{
			System.Collections.Generic.List<string> lstFiles = new System.Collections.Generic.List<string>();
			dtMostRecentUpdate = System.DateTime.MinValue;

			// Find files in diFolder
			foreach (System.IO.FileInfo fiFile in diFolder.GetFiles("*.*", System.IO.SearchOption.AllDirectories))
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
		/// <param name="serverFiles">Files that were found for this dataset on the server</param>
		/// <param name="udtDatasetInfo">Dataset info</param>
		/// <param name="lstJobsToPurge">Jobs whose folders will be deleted</param>
		/// <returns>List of files that are safe to delete</returns>
		public System.Collections.Generic.SortedSet<string> FindDatasetFilesToAutoPurge(clsStorageOperations.udtDatasetInfoType udtDatasetInfo, out System.Collections.Generic.List<int> lstJobsToPurge)
		{

			System.Collections.Generic.SortedSet<string> lstServerFilesToPurge = new System.Collections.Generic.SortedSet<string>();
			lstJobsToPurge = new System.Collections.Generic.List<int>();

			System.Text.RegularExpressions.Regex reJobFolder = new System.Text.RegularExpressions.Regex(@"_Auto(\d+)$", System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
			System.Text.RegularExpressions.Match reMatch;

			System.IO.DirectoryInfo diDatasetFolder = new System.IO.DirectoryInfo(udtDatasetInfo.ServerFolderPath);

			// Process files in the dataset folder

			switch (udtDatasetInfo.RawDataType)
			{
				case "dot_raw_files":
					AddFilesToPurge(diDatasetFolder, "*.raw", ref lstServerFilesToPurge);
					break;

				case "dot_wiff_files":
				case "sciex_wiff_files":
					AddFilesToPurge(diDatasetFolder, "*.wiff", ref lstServerFilesToPurge);
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
			foreach (System.IO.DirectoryInfo diSubDir in diDatasetFolder.GetDirectories())
			{
				string subDirNameUpper = diSubDir.Name.ToUpper();
				bool subDirProcessed = false;

				if (diSubDir.Name == "QC")
					// Do not purge the QC folder
					subDirProcessed = true;

				if (!subDirProcessed && subDirNameUpper.EndsWith(".D"))
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
					subDirProcessed = true;
				}

				if (!subDirProcessed && subDirNameUpper.EndsWith(".RAW"))
				{
					AddFilesToPurge(diSubDir, "*.raw", 0, true, ref lstServerFilesToPurge);

					// Purge all files over 2 MB in size
					AddFilesToPurge(diSubDir, "*.*", 2048, true, ref lstServerFilesToPurge);
					subDirProcessed = true;
				}

				if (!subDirProcessed && subDirNameUpper.StartsWith("MSXML_GEN"))
				{
					AddFilesToPurgeDateThreshold(diSubDir, 90, ref lstServerFilesToPurge);
					subDirProcessed = true;
				}

				if (!subDirProcessed && subDirNameUpper.StartsWith("DTA_GEN") || subDirNameUpper.StartsWith("DTA_REF"))
				{
					AddFilesToPurgeDateThreshold(diSubDir, 365, ref lstServerFilesToPurge);
					subDirProcessed = true;
				}

				if (!subDirProcessed)
				{
					reMatch = reJobFolder.Match(diSubDir.Name);
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
							bool bSubDirPurged = AddFilesToPurgeDateThreshold(diSubDir, 3 * 365, ref lstServerFilesToPurge);

							if (!bSubDirPurged)
							{
								// Files are not yet 3 years old
								// If all of the files are 1 year old, then purge files over 50 MB

								System.Collections.Generic.List<string> lstFiles;
								System.DateTime dtMostRecentUpdate;
								int iFilesMatched;

								lstFiles = FindFilesAndNewestDate(diSubDir, out dtMostRecentUpdate);

								if (System.DateTime.UtcNow.Subtract(dtMostRecentUpdate).TotalDays > 365)
								{
									// Purge all files over 50 MB in size
									iFilesMatched = AddFilesToPurge(diSubDir, "*.*", 50 * 1024, true, ref lstServerFilesToPurge);

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

						subDirProcessed = true;
					}

				}

				if (!subDirProcessed)
				{
					// Use a threshold of 9 months for all other subfolders
					AddFilesToPurgeDateThreshold(diSubDir, 270, ref lstServerFilesToPurge);
					subDirProcessed = true;
				}

			}

			return lstServerFilesToPurge;

		}

	}
}
