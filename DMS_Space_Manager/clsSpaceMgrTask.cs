//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************

using System;
using System.Data;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace Space_Manager
{
    /// <summary>
    /// Provides database access and tools for one capture task
    /// </summary>
    internal class clsSpaceMgrTask : clsDbTask, ITaskParams
    {
        #region "Constants"

        private const string SP_NAME_SET_COMPLETE = "SetPurgeTaskComplete";
        private const string SP_NAME_REQUEST_TASK = "RequestPurgeTask";

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager params for use by class</param>
        /// <param name="traceMode">True to show additional debug messages</param>
        public clsSpaceMgrTask(MgrSettings mgrParams, bool traceMode)
            : base(mgrParams, traceMode)
        {
            m_JobParams.Clear();
        }

        #region "Methods"

        /// <summary>
        /// Gets a stored parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public string GetParam(string name)
        {
            if (m_JobParams.TryGetValue(name, out var value))
            {
                return value;
            }

            return string.Empty;
        }

        /// <summary>
        /// Adds a parameter
        /// </summary>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Value for parameter</param>
        /// <returns>TRUE for success, FALSE for error</returns>
        public bool AddAdditionalParameter(string paramName, string paramValue)
        {
            try
            {
                SetParam(paramName, paramValue);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception adding parameter: " + paramName + ", Value: " + paramValue, ex);
                return false;
            }
        }

        /// <summary>
        /// Stores a parameter
        /// </summary>
        /// <param name="keyName">Parameter key</param>
        /// <param name="value">Parameter value</param>
        public void SetParam(string keyName, string value)
        {
            if (value == null)
            {
                value = string.Empty;
            }

            // Add/update the dictionary
            m_JobParams[keyName] = value;
        }

        /// <summary>
        /// Wrapper for requesting a task from the database
        /// </summary>
        /// <returns>num indicating if task was found</returns>
        public override EnumRequestTaskResult RequestTask(string driveLetter)
        {
            var retVal = RequestTaskDetailed(driveLetter);
            return retVal;
        }

        /// <summary>
        /// Detailed step request
        /// </summary>
        /// <returns>RequestTaskResult enum</returns>
        private EnumRequestTaskResult RequestTaskDetailed(string driveLetter)
        {
            EnumRequestTaskResult outcome;

            try
            {
                var dbTools = m_DMSProcedureExecutor;

                // Set up the command object prior to SP execution
                var cmd = dbTools.CreateCommand(SP_NAME_REQUEST_TASK, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                // ReSharper disable once StringLiteralTypo
                dbTools.AddParameter(cmd, "@StorageServerName", SqlType.VarChar, 128, m_MgrParams.GetParam("machname"));

                var serverDisk = driveLetter;
                if (driveLetter.EndsWith(":"))
                {
                    // Append back slash to drive letter
                    serverDisk = driveLetter + @"\";
                }

                dbTools.AddParameter(cmd, "@ServerDisk", SqlType.VarChar, 128, serverDisk);
                var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);
                dbTools.AddParameter(cmd, "@infoOnly", SqlType.TinyInt).Value = 0;

                // We stopped creating stagemd5 files in January 2016
                // Thus, pass 0 for this parameter instead of 1
                dbTools.AddParameter(cmd, "@ExcludeStageMD5RequiredDatasets", SqlType.TinyInt).Value = 0;

                var msg = "clsSpaceMgrTask.RequestTaskDetailed(), connection string: " + m_DMSProcedureExecutor.ConnectStr;
                LogTools.LogDebug(msg);

                msg = "clsSpaceMgrTask.RequestTaskDetailed(), printing param list";
                LogTools.LogDebug(msg);
                PrintCommandParams(cmd);

                // Execute the SP
                var retVal = m_DMSProcedureExecutor.ExecuteSPData(cmd, out var queryResults);

                switch (retVal)
                {
                    case RET_VAL_OK:
                        // Step task was found; get the data for it
                        var paramSuccess = FillParamDict(queryResults);
                        if (paramSuccess)
                        {
                            outcome = EnumRequestTaskResult.TaskFound;
                        }
                        else
                        {
                            // There was an error
                            outcome = EnumRequestTaskResult.ResultError;
                        }
                        break;
                    case RET_VAL_TASK_NOT_AVAILABLE:
                        // No jobs found
                        outcome = EnumRequestTaskResult.NoTaskFound;
                        break;
                    default:
                        // There was an SP error
                        LogError("clsSpaceMgrTask.RequestTaskDetailed(), SP execution error " + retVal +
                            "; Msg text = " + (string)messageParam.Value);
                        outcome = EnumRequestTaskResult.ResultError;
                        break;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception requesting analysis job", ex);
                outcome = EnumRequestTaskResult.ResultError;
            }

            return outcome;
        }

        public override void CloseTask(EnumCloseOutType taskResult)
        {
            var completionCode = (int)taskResult;

            if (!SetPurgeTaskComplete(SP_NAME_SET_COMPLETE, completionCode, m_JobParams["dataset"]))
            {
                LogError("Error setting task complete in database, dataset " + m_JobParams["dataset"]);
            }
            else
            {
                LogDebug("Successfully set task complete in database, dataset " + m_JobParams["dataset"]);
            }
        }

        /// <summary>
        /// Database calls to set a capture task complete
        /// </summary>
        /// <param name="spName">Name of SetComplete stored procedure</param>
        /// <param name="completionCode">
        /// Integer representation of completion code
        ///  0 = success
        ///  1 = Purge Failed
        ///  2 = Archive Update required
        ///  3 = Stage MD5 file required
        ///  4 = Drive Missing
        ///  5 = Purged Instrument Data (and any other auto-purge items)
        ///  6 = Purged all data except QC directory
        ///  7 = Dataset directory missing in archive
        ///  8 = Archive offline
        /// </param>
        /// <param name="datasetName">Dataset name</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        public bool SetPurgeTaskComplete(string spName, int completionCode, string datasetName)
        {
            // Setup for execution of the stored procedure
            var dbTools = m_DMSProcedureExecutor;
            var cmd = dbTools.CreateCommand(spName, CommandType.StoredProcedure);

            dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
            dbTools.AddParameter(cmd, "@datasetNum", SqlType.VarChar, 128, datasetName);
            dbTools.AddParameter(cmd, "@completionCode", SqlType.Int).Value = completionCode;
            dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);

            if (TraceMode)
            {
                clsUtilityMethods.ReportDebug(
                    string.Format("Calling {0} for dataset {1} with completion code {2}",
                                  spName, datasetName, completionCode));
            }

            // Execute the SP
            // Note that stored procedure SetPurgeTaskComplete (in DMS5) will call
            // MakeNewArchiveUpdateJob (in the DMS_Capture database) if the completionCode is 2 = Archive Update required
            var resCode = m_DMSProcedureExecutor.ExecuteSP(cmd);

            return resCode == 0;
        }

        #endregion
    }
}
