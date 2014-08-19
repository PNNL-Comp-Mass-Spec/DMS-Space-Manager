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
			protected readonly string m_ConnStr;
			protected readonly string m_BrokerConnStr;
			protected readonly List<string> m_ErrorList = new List<string>();
			protected bool m_TaskWasAssigned;
			protected readonly Dictionary<string, string> m_JobParams = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
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
			{	get 
				{ 
					return m_JobParams;
				} 
			}
		#endregion

		#region "Constructors"
			protected clsDbTask(IMgrParams MgrParams)
			{
				m_MgrParams = MgrParams;
				m_ConnStr = m_MgrParams.GetParam("ConnectionString");
				m_BrokerConnStr = m_MgrParams.GetParam("brokerconnectionstring");
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
			/// Reports database errors to local log
			/// </summary>
			protected void LogErrorEvents()
			{
				if (m_ErrorList.Count > 0)
				{
					const string msg = "Warning messages were posted to local log";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.WARN,msg);
				}
				foreach (string s in m_ErrorList)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, s);
				}
			}

			/// <summary>
			/// Method for executing a db stored procedure, assuming no data table is returned
			/// </summary>
			/// <param name="spCmd">SQL command object containing stored procedure params</param>
			/// <param name="connStr">Db connection string</param>
			/// <returns>Result code returned by SP; -1 if unable to execute SP</returns>
			protected int ExecuteSP(SqlCommand spCmd, string connStr)
			{
				DataTable dummyTable = null;
				return ExecuteSP(spCmd, ref dummyTable, connStr);
			}

			/// <summary>
			/// Method for executing a db stored procedure if a data table is to be returned
			/// </summary>
			/// <param name="spCmd">SQL command object containing stored procedure params</param>
			/// <param name="outTable">NOTHING when called; if SP successful, contains data table on return</param>
			/// <param name="connStr">Db connection string</param>
			/// <returns>Result code returned by SP; -1 if unable to execute SP</returns>
			protected int ExecuteSP(SqlCommand spCmd, ref DataTable outTable, string connStr)
			{
				//If this value is in error msg, then exception occurred before ResCode was set
				int resCode = -9999;
				
				string msg;
				var myTimer = new System.Diagnostics.Stopwatch();
				int retryCount = 3;

				m_ErrorList.Clear();
				while (retryCount > 0)
				{
					//Multiple retry loop for handling SP execution failures
					try
					{
						using (var cn = new SqlConnection(connStr))
						{
							cn.InfoMessage += OnInfoMessage;
							using (var da = new SqlDataAdapter())
							{
								using (var ds = new DataSet())
								{
									//NOTE: The connection has to be added here because it didn't exist at the time the command object was created
									spCmd.Connection = cn;
									//Change command timeout from 30 second default in attempt to reduce SP execution timeout errors
									spCmd.CommandTimeout = int.Parse(m_MgrParams.GetParam("cmdtimeout"));
									da.SelectCommand = spCmd;
									myTimer.Start();
									da.Fill(ds);
									myTimer.Stop();
									resCode = (int)da.SelectCommand.Parameters["@Return"].Value;
									if ((outTable != null) && (ds.Tables.Count>0)) outTable = ds.Tables[0];
								}	// ds
							}	//de
							cn.InfoMessage -= OnInfoMessage;
						}	// cn
						LogErrorEvents();
						break;
					}
					catch (Exception ex)
					{
						myTimer.Stop();
						retryCount -= 1;
						msg = "clsDBTask.ExecuteSP(), exception filling data adapter, " + ex.Message;
						msg += ". ResCode = " + resCode + ". Retry count = " + retryCount;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					}
					finally
					{
						//Log debugging info
						msg = "SP execution time: " + (myTimer.ElapsedMilliseconds / 1000.0).ToString("##0.000") + " seconds ";
						msg += "for SP " + spCmd.CommandText;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

						//Reset the connection timer
						myTimer.Reset();
					}
					//Wait 10 seconds before retrying
					System.Threading.Thread.Sleep(10000);
				}

				if (retryCount < 1)
				{
					//Too many retries, log and return error
					msg = "Excessive retries executing SP " + spCmd.CommandText;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return -1;
				}

				return resCode;
			}

			/// <summary>
			/// Debugging routine for printing SP calling params
			/// </summary>
			/// <param name="inpCmd">SQL command object containing params</param>
			protected void PrintCommandParams(SqlCommand inpCmd)
			{
				//Verify there really are command paramters
				if (inpCmd == null) return;

				if (inpCmd.Parameters.Count < 1) return;

				string myMsg = "";

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
				// SambaStoragePath: \\a2.emsl.pnl.gov\dmsarch\VOrbiETD02\2011_3
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

		#region "Event handlers"
			/// <summary>
			/// Event handler for InfoMessage event from SQL Server
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="args"></param>
			private void OnInfoMessage(object sender, SqlInfoMessageEventArgs args)
			{
				var errString = new StringBuilder();
				foreach (SqlError err in args.Errors)
				{
					errString.Length = 0;
					errString.Append("Message: " + err.Message);
					errString.Append(", Source: " + err.Source);
					errString.Append(", Class: " + err.Class);
					errString.Append(", State: " + err.State);
					errString.Append(", Number: " + err.Number);
					errString.Append(", LineNumber: " + err.LineNumber);
					errString.Append(", Procedure:" + err.Procedure);
					errString.Append(", Server: " + err.Server);
					m_ErrorList.Add(errString.ToString());
				}
			}
		#endregion
	}	// End class
}	// End namespace
