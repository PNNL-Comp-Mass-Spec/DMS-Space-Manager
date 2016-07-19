//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace Space_Manager
{
    abstract class clsDbTask
    {
        //*********************************************************************************************************
        // Base class for handling task-related data
        //**********************************************************************************************************

        #region "Constants"
        protected const int RET_VAL_OK = 0;
        protected const int RET_VAL_TASK_NOT_AVAILABLE = -53000;
        #endregion

        #region "Class variables"
        protected readonly IMgrParams m_MgrParams;
        protected bool m_TaskWasAssigned;
        protected readonly Dictionary<string, string> m_JobParams = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

        protected PRISM.DataBase.clsExecuteDatabaseSP DMSProcedureExecutor;

        #endregion

        #region "Properties"
        public bool TaskWasAssigned
        {
            get
            {
                return m_TaskWasAssigned;
            }
        }

        public Dictionary<string, string> TaskDictionary
        {
            get
            {
                return m_JobParams;
            }
        }
        #endregion

        #region "Constructors"

        protected clsDbTask(IMgrParams mgrParams)
        {
            m_MgrParams = mgrParams;

            // This Connection String points to the DMS5 database
            var connectionString = m_MgrParams.GetParam("ConnectionString");
            DMSProcedureExecutor = new PRISM.DataBase.clsExecuteDatabaseSP(connectionString);
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
        protected void PrintCommandParams(SqlCommand inpCmd)
        {
            //Verify there really are command paramters
            if (inpCmd == null) return;

            if (inpCmd.Parameters.Count < 1) return;

            var myMsg = "";

            foreach (SqlParameter myParam in inpCmd.Parameters)
            {
                myMsg += Environment.NewLine + "Name= " + myParam.ParameterName + "\t, Value= " + DbCStr(myParam.Value);
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parameter list:" + myMsg);
        }

        protected bool FillParamDict(DataTable dt)
        {
            string msg;

            // Verify valid datatable
            if (dt == null)
            {
                msg = "clsDbTask.FillParamDict(): No parameter table";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }

            // Verify at least one row present
            if (dt.Rows.Count < 1)
            {
                msg = "clsDbTask.FillParamDict(): No parameters returned by request SP";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
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
            // SambaStoragePath: \\aurora.emsl.pnl.gov\archive\dmsarch\VOrbiETD02\2011_3
            // Instrument: VOrbiETD02
            // DatasetCreated: 2011-08-29 13:42:05
            // DatasetYearQuarter: 2011_3

            m_JobParams.Clear();
            try
            {
                foreach (DataRow currRow in dt.Rows)
                {
                    var myKey = currRow[dt.Columns["Parameter"]] as string;
                    var myVal = currRow[dt.Columns["Value"]] as string;

                    if (myKey != null)
                    {
                        m_JobParams.Add(myKey, myVal);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                msg = "clsDbTask.FillParamDict(): Exception reading task parameters";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                return false;
            }
        }

        protected string DbCStr(object InpObj)
        {
            //If input object is DbNull, returns "", otherwise returns String representation of object
            if ((InpObj == null) || (object.ReferenceEquals(InpObj, DBNull.Value)))
            {
                return "";
            }
            else
            {
                return InpObj.ToString();
            }
        }

        protected float DbCSng(object InpObj)
        {
            //If input object is DbNull, returns 0.0, otherwise returns Single representation of object
            if (object.ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0F;
            }
            else
            {
                return (float)InpObj;
            }
        }

        protected double DbCDbl(object InpObj)
        {
            //If input object is DbNull, returns 0.0, otherwise returns Double representation of object
            if (object.ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0;
            }
            else
            {
                return (double)InpObj;
            }
        }

        protected int DbCInt(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (object.ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            else
            {
                return (int)InpObj;
            }
        }

        protected long DbCLng(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (object.ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            else
            {
                return (long)InpObj;
            }
        }

        protected decimal DbCDec(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Decimal representation of object
            if (object.ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            else
            {
                return (decimal)InpObj;
            }
        }

        protected short DbCShort(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Short representation of object
            if (object.ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            else
            {
                return (short)InpObj;
            }
        }
        #endregion
     
    }
}
