//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 02/01/2011
//
//*********************************************************************************************************

using System;

namespace Space_Manager
{
    // Namespace level values for application

    #region "Enums"

    /// <summary>
    /// Manager status constants
    /// </summary>
    public enum EnumMgrStatus : short
    {
        Stopped,
        Stopped_Error,
        Running,
        Disabled_Local,
        Disabled_MC
    }

    /// <summary>
    /// Task status constants
    /// </summary>
    public enum EnumTaskStatus : short
    {
        Stopped,
        Requesting,
        Running,
        Closing,
        Failed,
        No_Task
    }

    /// <summary>
    /// Task detail constants
    /// </summary>
    public enum EnumTaskStatusDetail : short
    {
        Retrieving_Resources,
        Running_Tool,
        Packaging_Results,
        Delivering_Results,
        No_Task
    }

    /// <summary>
    /// Purge task completion codes
    /// </summary>
    /// <remarks>These codes are used by stored procedure SetPurgeTaskComplete</remarks>
    public enum EnumCloseOutType : short
    {
        CLOSEOUT_SUCCESS = 0,                   // Purged all data; PurgePolicy=2
        CLOSEOUT_FAILED = 1,
        CLOSEOUT_UPDATE_REQUIRED = 2,           // This completion code will cause SetPurgeTaskComplete to auto-call MakeNewArchiveUpdateJob in the DMS_Capture database
                                                // Obsolete: CLOSEOUT_WAITING_HASH_FILE = 3,
        CLOSEOUT_DRIVE_MISSING = 4,
        CLOSEOUT_PURGE_AUTO = 5,                // Purged instrument data, MSXML data, and older jobs; PurgePolicy=0
        CLOSEOUT_PURGE_ALL_EXCEPT_QC = 6,       // Purged instrument data, MSXML data, and older jobs; PurgePolicy=0
        CLOSEOUT_DATASET_FOLDER_MISSING_IN_ARCHIVE = 7,
        CLOSEOUT_ARCHIVE_OFFLINE = 8,           // Previously CLOSEOUT_AURORA_OFFLINE
        CLOSEOUT_PREVIEWED_PURGE = 9
    }

    /// <summary>
    /// Request task result codes
    /// </summary>
    public enum EnumRequestTaskResult : short
    {
        TaskFound = 0,
        NoTaskFound = 1,
        ResultError = 2,
        ConfigChanged = 3
    }

    /// <summary>
    /// Space check result codes
    /// </summary>
    public enum SpaceCheckResults
    {
        Error = -1,
        Above_Threshold = 1,
        Below_Threshold = 0
    }

    #endregion

    #region "Delegates"

    public delegate void StatusMonitorUpdateReceived(string msg);

    #endregion

    /// <summary>
    /// Conversion utilities
    /// </summary>
    public static class clsConversion
    {
        /// <summary>
        /// Convert string to bool; default false if an error
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool CBoolSafe(string value)
        {
            return CBoolSafe(value, false);
        }

        public static bool CBoolSafe(string value, bool defaultValue)
        {
            if (string.IsNullOrEmpty(value))
                return defaultValue;

            if (bool.TryParse(value, out var blnValue))
                return blnValue;

            return defaultValue;
        }

        public static int CIntSafe(string value, int defaultValue)
        {
            if (string.IsNullOrEmpty(value))
                return defaultValue;

            if (int.TryParse(value, out var intValue))
                return intValue;

            return defaultValue;
        }

        public static float CSngSafe(string value, float defaultValue)
        {
            var fValue = defaultValue;

            if (string.IsNullOrEmpty(value))
                return fValue;

            if (float.TryParse(value, out fValue))
                return fValue;

            return fValue;
        }

        public static string PossiblyQuotePath(string strPath)
        {
            if (string.IsNullOrEmpty(strPath))
            {
                return string.Empty;
            }

            if (strPath.Contains(" "))
            {
                if (!strPath.StartsWith("\""))
                {
                    strPath = "\"" + strPath;
                }

                if (!strPath.EndsWith("\""))
                {
                    strPath += "\"";
                }
            }

            return strPath;
        }
    }
}
