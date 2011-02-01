
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
// Last modified 09/09/2010
//*********************************************************************************************************
using System;
using System.Data.SqlClient;
using System.Data;
using System.Windows.Forms;

namespace Space_Manager
{
	class clsSpaceMgrTask : clsDbTask, ITaskParams
	{
		//*********************************************************************************************************
		// Provides database access and tools for one capture task
		//**********************************************************************************************************

		#region "Constants"
			//TODO: Correct these when SP's are finalized
			protected const string SP_NAME_SET_COMPLETE = "SetPurgeTaskComplete";
			protected const string SP_NAME_REQUEST_TASK = "RequestPurgeTask";
		#endregion

		#region "Class variables"
//			int m_JobID = 0;
		#endregion

		#region "Constructors"
			/// <summary>
			/// Class constructor
			/// </summary>
			/// <param name="mgrParams">Manager params for use by class</param>
			public clsSpaceMgrTask(IMgrParams mgrParams)
				: base(mgrParams)
			{
				m_JobParams.Clear();
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Gets a stored parameter
			/// </summary>
			/// <param name="name">Parameter name</param>
			/// <returns>Parameter value if found, otherwise empty string</returns>
			public string GetParam(string name)
			{
				if (m_JobParams.ContainsKey(name))
				{
					return m_JobParams[name];
				}
				else
				{
					return "";
				}
			}	// End sub

			/// <summary>
			/// Adds a parameter
			/// </summary>
			/// <param name="paramName">Name of parameter</param>
			/// <param name="paramValue">Value for parameter</param>
			/// <returns>RUE for success, FALSE for error</returns>
			public bool AddAdditionalParameter(string paramName, string paramValue)
			{
				try
				{
					m_JobParams.Add(paramName, paramValue);
					return true;
				}
				catch (Exception ex)
				{
					string msg = "Exception adding parameter: " + paramName + ", Value: " + paramValue;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Stores a parameter
			/// </summary>
			/// <param name="keyName">Parameter key</param>
			/// <param name="value">Parameter value</param>
			public void SetParam(string keyName, string value)
			{
				if (value == null)
				{
					value = "";
				}
				m_JobParams[keyName] = value;
			}	// End sub

			/// <summary>
			/// Wrapper for requesting a task from the database
			/// </summary>
			/// <returns>num indicating if task was found</returns>
			public override EnumRequestTaskResult RequestTask(string driveLetter)
			{
				EnumRequestTaskResult retVal;

				retVal = RequestTaskDetailed(driveLetter);
				switch (retVal)
				{
					case EnumRequestTaskResult.TaskFound:
						m_TaskWasAssigned = true;
						break;
					case EnumRequestTaskResult.NoTaskFound:
						m_TaskWasAssigned = false;
						break;
					default:
						m_TaskWasAssigned = false;
						break;
				}

				return retVal;
			}	// End sub

			/// <summary>
			/// Detailed step request
			/// </summary>
			/// <returns>RequestTaskResult enum</returns>
			private EnumRequestTaskResult RequestTaskDetailed(string driveLetter)
			{
				string msg;
				SqlCommand myCmd = new SqlCommand();
				EnumRequestTaskResult outcome = EnumRequestTaskResult.NoTaskFound;
				int retVal = 0;
				DataTable dt = new DataTable();
				//string strProductVersion = Application.ProductVersion;
				//if (strProductVersion == null) strProductVersion = "??";

				try
				{
					//Set up the command object prior to SP execution
					{
						myCmd.CommandType = CommandType.StoredProcedure;
						myCmd.CommandText = SP_NAME_REQUEST_TASK;
						myCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
						myCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

						myCmd.Parameters.Add(new SqlParameter("@StorageServerName", SqlDbType.VarChar, 128));
						myCmd.Parameters["@StorageServerName"].Direction = ParameterDirection.Input;
						myCmd.Parameters["@StorageServerName"].Value = m_MgrParams.GetParam("machname");

						myCmd.Parameters.Add(new SqlParameter("@ServerDisk", SqlDbType.VarChar, 128));
						myCmd.Parameters["@ServerDisk"].Direction = ParameterDirection.Input;
						if (driveLetter.EndsWith(":"))
						{
							// Append back slash to drive letter
							myCmd.Parameters["@ServerDisk"].Value = driveLetter + @"\";
						}
						else myCmd.Parameters["@ServerDisk"].Value = driveLetter;

						myCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512));
						myCmd.Parameters["@message"].Direction = ParameterDirection.Output;
						myCmd.Parameters["@message"].Value = "";

						myCmd.Parameters.Add(new SqlParameter("@infoOnly", SqlDbType.TinyInt));
						myCmd.Parameters["@infoOnly"].Direction = ParameterDirection.Input;
						myCmd.Parameters["@infoOnly"].Value = 0;

						myCmd.Parameters.Add(new SqlParameter("@ExcludeStageMD5RequiredDatasets", SqlDbType.TinyInt));
						myCmd.Parameters["@ExcludeStageMD5RequiredDatasets"].Direction = ParameterDirection.Input;
						myCmd.Parameters["@ExcludeStageMD5RequiredDatasets"].Value = 1;
					}

					msg = "clsSpaceMgrTask.RequestTaskDetailed(), connection string: " + m_BrokerConnStr;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					msg = "clsSpaceMgrTask.RequestTaskDetailed(), printing param list";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					PrintCommandParams(myCmd);

					//Execute the SP
					retVal = ExecuteSP(myCmd, ref dt, m_ConnStr);

					switch (retVal)
					{
						case RET_VAL_OK:
							//Step task was found; get the data for it
							bool paramSuccess = FillParamDict(dt);
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
							//There was an SP error
							msg = "clsSpaceMgrTask.RequestTaskDetailed(), SP execution error " + retVal.ToString();
							msg += "; Msg text = " + (string)myCmd.Parameters["@message"].Value;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
							outcome = EnumRequestTaskResult.ResultError;
							break;
					}
				}
				catch (System.Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception requesting analysis job: " + ex.Message);
					outcome = EnumRequestTaskResult.ResultError;
				}
				return outcome;
			}	// End sub

			public override void CloseTask(EnumCloseOutType taskResult)
			{
				string msg;
				int compCode = (int)taskResult;

				if (!SetPurgeTaskComplete(SP_NAME_SET_COMPLETE, m_ConnStr, compCode, m_JobParams["dataset"]))
				{
					msg = "Error setting task complete in database, dataset " + m_JobParams["dataset"];
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
				else
				{
					msg = msg = "Successfully set task complete in database, dataset " + m_JobParams["dataset"];
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}
			}	// End sub

			/// <summary>
			/// Database calls to set a capture task complete
			/// </summary>
			/// <param name="SpName">Name of SetComplete stored procedure</param>
			/// <param name="CompletionCode">Integer representation of completion code</param>
			/// <param name="ConnStr">Db connection string</param>
			/// <returns>TRUE for sucesss; FALSE for failure</returns>
			public bool SetPurgeTaskComplete(string spName, string connStr, int compCode, string datasetName)
			{
				string msg;
				bool Outcome = false;
				int ResCode = 0;

				//Setup for execution of the stored procedure
				SqlCommand MyCmd = new SqlCommand();
				{
					MyCmd.CommandType = CommandType.StoredProcedure;
					MyCmd.CommandText = spName;
					MyCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
					MyCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;
					MyCmd.Parameters.Add(new SqlParameter("@datasetNum", SqlDbType.VarChar, 128));
					MyCmd.Parameters["@datasetNum"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@datasetNum"].Value = datasetName;
					MyCmd.Parameters.Add(new SqlParameter("@completionCode", SqlDbType.Int));
					MyCmd.Parameters["@completionCode"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@completionCode"].Value = compCode;
					MyCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512));
					MyCmd.Parameters["@message"].Direction = ParameterDirection.Output;
				}

				//Execute the SP
				ResCode = ExecuteSP(MyCmd, connStr);

				if (ResCode == 0)
				{
					Outcome = true;
				}
				else
				{
					msg = "Error " + ResCode.ToString() + " setting purge task complete";
					msg += "; Message = " + (string)MyCmd.Parameters["@message"].Value;
					Outcome = false;
				}
				return Outcome;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
