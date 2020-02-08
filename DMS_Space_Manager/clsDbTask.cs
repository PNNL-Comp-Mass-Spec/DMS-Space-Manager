//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data.Common;
using PRISM.AppSettings;

namespace Space_Manager
{
    /// <summary>
    /// Base class for handling task-related data
    /// </summary>
    abstract class clsDbTask : clsLoggerBase
    {

        #region "Constants"

        protected const int RET_VAL_OK = 0;
        protected const int RET_VAL_TASK_NOT_AVAILABLE = 53000;

        #endregion

        #region "Class variables"

        protected readonly MgrSettings m_MgrParams;

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>4 means Info level (normal) logging; 5 for Debug level (verbose) logging</remarks>
        protected readonly int m_DebugLevel;

        /// <summary>
        /// Job parameters
        /// </summary>
        protected readonly Dictionary<string, string> m_JobParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Stored procedure executor
        /// </summary>
        protected readonly PRISMDatabaseUtils.IDBTools m_DMSProcedureExecutor;

        #endregion

        #region "Properties"

        public Dictionary<string, string> TaskDictionary => m_JobParams;

        public bool TraceMode { get; }

        #endregion

        #region "Constructor"

        /// <summary>
        /// Class constructor
        /// </summary>
        /// <param name="mgrParams">Manager params for use by class</param>
        /// <param name="traceMode">True to show additional debug messages</param>
        protected clsDbTask(MgrSettings mgrParams, bool traceMode)
        {
            m_MgrParams = mgrParams;

            TraceMode = traceMode;

            // This Connection String points to the DMS5 database
            var connectionString = m_MgrParams.GetParam("ConnectionString");
            m_DMSProcedureExecutor = PRISMDatabaseUtils.DbToolsFactory.GetDBTools(connectionString);

            m_DMSProcedureExecutor.ErrorEvent += DMSProcedureExecutor_DBErrorEvent;

            // Cache the log level
            // 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            m_DebugLevel = mgrParams.GetParam("DebugLevel", 4);
        }

        #endregion

        #region "Methods"
        /// <summary>
        /// Requests a task
        /// </summary>
        /// <returns>RequestTaskResult enum specifying call result</returns>
        public abstract EnumRequestTaskResult RequestTask(string driveLetter);

        /// <summary>
        /// Closes a task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        public abstract void CloseTask(EnumCloseOutType taskResult);

        /// <summary>
        /// Debugging routine for printing SP calling params
        /// </summary>
        /// <param name="inpCmd">SQL command object containing params</param>
        protected void PrintCommandParams(DbCommand inpCmd)
        {
            // Verify there really are command parameters
            if (inpCmd == null)
                return;

            if (inpCmd.Parameters.Count < 1)
                return;

            var msg = "";

            foreach (DbParameter myParam in inpCmd.Parameters)
            {
                msg += Environment.NewLine + string.Format("  Name= {0,-20}, Value= {1}", myParam.ParameterName, DbCStr(myParam.Value));
            }

            var writeToLog = m_DebugLevel >= 5;
            LogDebug("Parameter list:" + msg, writeToLog);
        }

        /// <summary>
        /// Fill string dictionary with parameter values
        /// </summary>
        /// <param name="parameters">Result table from call to RequestStepTask</param>
        /// <returns></returns>
        protected bool FillParamDict(List<List<string>> parameters)
        {
            // Verify valid parameters
            if (parameters == null)
            {
                LogError("clsDbTask.FillParamDict(): parameters is null");
                return false;
            }

            // Verify at least one row present
            if (parameters.Count < 1)
            {
                LogError("clsDbTask.FillParamDict(): No parameters returned by request SP");
                return false;
            }

            // Fill string dictionary with parameter values

            // Example parameters:
            // dataset: 08232011_UBQ_500MS_AGC_1E6_01
            // DatasetID: 237350
            // Folder: 08232011_UBQ_500MS_AGC_1E6_01
            // StorageVol: G:\
            // storagePath: VOrbiETD02\2011_3\
            // StorageVolExternal: \\proto-9\
            // RawDataType: dot_raw_files
            // SambaStoragePath: \\agate.emsl.pnl.gov\dmsarch\VOrbiETD02\2011_3
            // Instrument: VOrbiETD02
            // DatasetCreated: 2011-08-29 13:42:05
            // DatasetYearQuarter: 2011_3

            m_JobParams.Clear();

            try
            {
                foreach (var dataRow in parameters)
                {
                    if (dataRow.Count < 2)
                        continue;

                    var paramName = dataRow[0];
                    var paramValue = dataRow[1];

                    if (!string.IsNullOrWhiteSpace(paramName))
                    {
                        m_JobParams.Add(paramName, paramValue);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                LogError("clsDbTask.FillParamDict(): Exception reading task parameters", ex);
                return false;
            }
        }

        private string DbCStr(object InpObj)
        {
            // If input object is DbNull, returns "", otherwise returns String representation of object
            if (InpObj == null || ReferenceEquals(InpObj, DBNull.Value))
            {
                return "";
            }

            return InpObj.ToString();
        }

        protected float DbCSng(object InpObj)
        {
            // If input object is DbNull, returns 0.0, otherwise returns Single representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0F;
            }

            return (float)InpObj;
        }

        protected double DbCDbl(object InpObj)
        {
            // If input object is DbNull, returns 0.0, otherwise returns Double representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0;
            }

            return (double)InpObj;
        }

        protected int DbCInt(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (int)InpObj;
        }

        protected long DbCLng(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (long)InpObj;
        }

        protected decimal DbCDec(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Decimal representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (decimal)InpObj;
        }

        protected short DbCShort(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Short representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (short)InpObj;
        }

        #endregion

        #region "Event handlers"

        void DMSProcedureExecutor_DBErrorEvent(string message, Exception ex)
        {
            var logToDb = message.Contains("permission was denied");

            if (logToDb)
                LogError(message, logToDb: true);
            else
                LogError(message, ex);
        }

        #endregion
    }
}
