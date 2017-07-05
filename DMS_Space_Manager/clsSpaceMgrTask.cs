
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************
using System;
using System.Data.SqlClient;
using System.Data;

namespace Space_Manager
{
    class clsSpaceMgrTask : clsDbTask, ITaskParams
    {
        //*********************************************************************************************************
        // Provides database access and tools for one capture task
        //**********************************************************************************************************

        #region "Constants"

        private const string SP_NAME_SET_COMPLETE = "SetPurgeTaskComplete";
        private const string SP_NAME_REQUEST_TASK = "RequestPurgeTask";
        #endregion

        #region "Class variables"

        #endregion

        #region "Constructors"

        /// <summary>
        /// Class constructor
        /// </summary>
        /// <param name="mgrParams">Manager params for use by class</param>
        /// <param name="traceMode">True to show additional debug messages</param>
        public clsSpaceMgrTask(IMgrParams mgrParams, bool traceMode)
            : base(mgrParams, traceMode)
        {
            m_JobParams.Clear();
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Gets a stored parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public string GetParam(string name)
        {
            string value;
            if (m_JobParams.TryGetValue(name, out value))
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
                value = String.Empty;
            }

            if (m_JobParams.ContainsKey(keyName))
                m_JobParams[keyName] = value;
            else
                m_JobParams.Add(keyName, value);

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

            //string strProductVersion = Application.ProductVersion;
            //if (strProductVersion == null) strProductVersion = "??";

            try
            {
                //Set up the command object prior to SP execution
                var myCmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = SP_NAME_REQUEST_TASK
                };

                myCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

                myCmd.Parameters.Add(new SqlParameter("@StorageServerName", SqlDbType.VarChar, 128)).Value = m_MgrParams.GetParam("machname");

                string serverDisk;

                if (driveLetter.EndsWith(":"))
                {
                    // Append back slash to drive letter
                    serverDisk = driveLetter + @"\";
                }
                else
                    serverDisk = driveLetter;

                myCmd.Parameters.Add(new SqlParameter("@ServerDisk", SqlDbType.VarChar, 128)).Value = serverDisk;

                myCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                myCmd.Parameters.Add(new SqlParameter("@infoOnly", SqlDbType.TinyInt)).Value = 0;

                // We stopped creating stagemd5 files in January 2016
                // Thus, pass 0 for this parameter instead of 1
                myCmd.Parameters.Add(new SqlParameter("@ExcludeStageMD5RequiredDatasets", SqlDbType.TinyInt)).Value = 0;

                var msg = "clsSpaceMgrTask.RequestTaskDetailed(), connection string: " + DMSProcedureExecutor.DBconnectionString;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

                msg = "clsSpaceMgrTask.RequestTaskDetailed(), printing param list";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                PrintCommandParams(myCmd);

                //Execute the SP
                var retVal = DMSProcedureExecutor.ExecuteSP(myCmd, out var queryResults);

                switch (retVal)
                {
                    case RET_VAL_OK:
                        //Step task was found; get the data for it
                        var paramSuccess = FillParamDict(queryResults);
                        if (paramSuccess)
                        {
                            outcome = EnumRequestTaskResult.TaskFound;
                        }
                        else
                        {
                            //There was an error
                            outcome = EnumRequestTaskResult.ResultError;
                        }
                        break;
                    case RET_VAL_TASK_NOT_AVAILABLE:
                        //No jobs found
                        outcome = EnumRequestTaskResult.NoTaskFound;
                        break;
                    default:
                        // There was an SP error
                        LogError("clsSpaceMgrTask.RequestTaskDetailed(), SP execution error " + retVal +
                            "; Msg text = " + (string)myCmd.Parameters["@message"].Value);
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
                ReportStatus("Successfully set task complete in database, dataset " + m_JobParams["dataset"], true);
            }
        }

        /// <summary>
        /// Database calls to set a capture task complete
        /// </summary>
        /// <param name="spName">Name of SetComplete stored procedure</param>
        /// <param name="completionCode">Integer representation of completion code</param>
        /// <param name="datasetName">Dataset name</param>
        /// <returns>TRUE for sucesss; FALSE for failure</returns>
        public bool SetPurgeTaskComplete(string spName, int completionCode, string datasetName)
        {

            //Setup for execution of the stored procedure
            var myCmd = new SqlCommand
            {
                CommandType = CommandType.StoredProcedure,
                CommandText = spName
            };

            myCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

            myCmd.Parameters.Add(new SqlParameter("@datasetNum", SqlDbType.VarChar, 128)).Value = datasetName;

            myCmd.Parameters.Add(new SqlParameter("@completionCode", SqlDbType.Int)).Value = completionCode;

            myCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

            if (TraceMode)
            {
                clsUtilityMethods.ReportDebug(
                    string.Format("Calling {0} for dataset {1} with completion code {2}",
                                  spName, datasetName, completionCode));
            }

            // Execute the SP
            // Note that stored procedure SetPurgeTaskComplete (in DMS5) will call 
            // MakeNewArchiveUpdateJob (in the DMS_Capture database) if the completionCode is 2 = Archive Update required
            var resCode = DMSProcedureExecutor.ExecuteSP(MyCmd);

            if (resCode == 0)
            {
                return true;
            }

            return false;

        }

        #endregion
    }
}
