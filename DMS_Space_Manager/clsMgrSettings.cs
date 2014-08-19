﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/08/2010
//
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Windows.Forms;
using System.Xml;

namespace Space_Manager
{
	public class clsMgrSettings : IMgrParams
	{
		//*********************************************************************************************************
		//	Class for loading, storing and accessing manager parameters.
		//	Loads initial settings from local config file, then checks to see if remainder of settings should be
		//		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
		//		parameters database.
		//**********************************************************************************************************

		#region "Class variables"
		Dictionary<string, string> m_ParamDictionary;
		bool m_MCParamsLoaded;
		#endregion

		#region "Properties"
		public string ErrMsg { get; set; }
		#endregion

		#region "Methods"
		public clsMgrSettings()
		{
			if (!LoadSettings())
			{
				throw new ApplicationException("Unable to initialize manager settings class");
			}
		}

		public bool LoadSettings()
		{
			ErrMsg = "";

			// If the param dictionary exists, it needs to be cleared out
			if (m_ParamDictionary != null)
			{
				m_ParamDictionary.Clear();
				m_ParamDictionary = null;
			}


			// Get settings from config file
			m_ParamDictionary = LoadMgrSettingsFromFile();

			// Get directory for main executable
			string appPath = Application.ExecutablePath;
			var fi = new FileInfo(appPath);
			m_ParamDictionary.Add("ApplicationPath", fi.DirectoryName);

			//Test the settings retrieved from the config file
			if (!CheckInitialSettings(m_ParamDictionary))
			{
				//Error logging handled by CheckInitialSettings
				return false;
			}

			//Determine if manager is deactivated locally
			if (!bool.Parse(m_ParamDictionary["MgrActive_Local"]))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.WARN, "Manager deactivated locally");
				ErrMsg = "Manager deactivated locally";
				return false;
			}

			//Get remaining settings from database
			if (!LoadMgrSettingsFromDB(ref m_ParamDictionary))
			{
				//Error logging handled by LoadMgrSettingsFromDB
				return false;
			}

			// Set flag indicating params have been loaded from manger config db
			m_MCParamsLoaded = true;

			//No problems found
			return true;
		}

		private Dictionary<string, string> LoadMgrSettingsFromFile()
		{
			// Load initial settings into string dictionary for return
			var RetDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

			//				My.Settings.Reload()
			//Manager config db connection string
			string TempStr = Properties.Settings.Default.MgrCnfgDbConnectStr;
			RetDict.Add("MgrCnfgDbConnectStr", TempStr);

			//Manager active flag
			TempStr = Properties.Settings.Default.MgrActive_Local.ToString();
			RetDict.Add("MgrActive_Local", TempStr);

			//Manager name
			TempStr = Properties.Settings.Default.MgrName;
			RetDict.Add("MgrName", TempStr);

			//Default settings in use flag
			TempStr = Properties.Settings.Default.UsingDefaults.ToString();
			RetDict.Add("UsingDefaults", TempStr);

			return RetDict;
		}

		private bool CheckInitialSettings(Dictionary<string, string> InpDict)
		{
			string MyMsg;

			//Verify manager settings dictionary exists
			if (InpDict == null)
			{
				MyMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, MyMsg);
				return false;
			}

			//Verify intact config file was found
			if (bool.Parse(InpDict["UsingDefaults"]))
			{
				MyMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, default settings being used";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, MyMsg);
				return false;
			}

			//No problems found
			return true;
		}

		public bool LoadMgrSettingsFromDB()
		{
			return LoadMgrSettingsFromDB(ref m_ParamDictionary);
		}

		public bool LoadMgrSettingsFromDB(ref Dictionary<string, string> MgrSettingsDict)
		{
			//Requests manager parameters from database. Input string specifies view to use. Performs retries if necessary.
			short RetryCount = 3;
			string MyMsg;

			string SqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + m_ParamDictionary["MgrName"] + "'";

			//Get a table containing data for job
			DataTable Dt = null;

			//Get a datatable holding the parameters for one manager
			while (RetryCount > 0)
			{
				try
				{
					using (var Cn = new SqlConnection(MgrSettingsDict["MgrCnfgDbConnectStr"]))
					{
						using (var Da = new SqlDataAdapter(SqlStr, Cn))
						{
							using (var Ds = new DataSet())
							{
								Da.Fill(Ds);
								Dt = Ds.Tables[0];
							}							
						}
					}
					
					break;
				}
				catch (Exception ex)
				{
					RetryCount -= 1;
					MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " + ex.Message;
					MyMsg = MyMsg + ", RetryCount = " + RetryCount;
					WriteErrorMsg(MyMsg);
					//Delay for 5 seconds before trying again
					System.Threading.Thread.Sleep(5000);
				}
			}

			//If loop exited due to errors, return false
			if (RetryCount < 1)
			{
				MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
				WriteErrorMsg(MyMsg);
				if (Dt != null)
				{
					Dt.Dispose();
				}
				return false;
			}

			if (Dt == null)
			{
				return false;
			}
			
			//Verify at least one row returned
			if (Dt.Rows.Count < 1)
			{
				//Wrong number of rows returned
				MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Invalid row count retrieving manager settings: RowCount = ";
				MyMsg += Dt.Rows.Count.ToString(CultureInfo.InvariantCulture);
				WriteErrorMsg(MyMsg);
				Dt.Dispose();
				return false;
			}

			//Fill a string dictionary with the manager parameters that have been found
			try
			{
				foreach (DataRow TestRow in Dt.Rows)
				{
					//Add the column heading and value to the dictionary
					string ParamKey = DbCStr(TestRow[Dt.Columns["ParameterName"]]);
					string ParamVal = DbCStr(TestRow[Dt.Columns["ParameterValue"]]);
					if (m_ParamDictionary.ContainsKey(ParamKey))
					{
						m_ParamDictionary[ParamKey] = ParamVal;
					}
					else
					{
						m_ParamDictionary.Add(ParamKey, ParamVal);
					}
				}
				return true;
			}
			catch (Exception ex)
			{
				MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " + ex.Message;
				WriteErrorMsg(MyMsg);
				return false;
			}
			finally
			{
				Dt.Dispose();
			}
		}

		public string GetParam(string ItemKey)
		{
			string RetStr;
			if (m_ParamDictionary.TryGetValue(ItemKey, out RetStr))
				return RetStr;
			
			return string.Empty;
		}

		public void SetParam(string ItemKey, string ItemValue)
		{
			m_ParamDictionary[ItemKey] = ItemValue;
		}

		/// <summary>
		/// Writes specfied value to an application config file.
		/// </summary>
		/// <param name="Key">Name for parameter (case sensitive)</param>
		/// <param name="Value">New value for parameter</param>
		/// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
		/// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
		public bool WriteConfigSetting(string Key, string Value)
		{

			ErrMsg = "";

			//Load the config document
			XmlDocument MyDoc = LoadConfigDocument();
			if (MyDoc == null)
			{
				//Error message has already been produced by LoadConfigDocument
				return false;
			}

			//Retrieve the settings node
			XmlNode MyNode = MyDoc.SelectSingleNode("//applicationSettings");

			if (MyNode == null)
			{
				ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
				return false;
			}

			try
			{
				//Select the eleement containing the value for the specified key containing the key
				var MyElement = (XmlElement)MyNode.SelectSingleNode(string.Format("//setting[@name='{0}']/value", Key));
				if (MyElement != null)
				{
					//Set key to specified value
					MyElement.InnerText = Value;
				}
				else
				{
					//Key was not found
					ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + Key;
					return false;
				}
				MyDoc.Save(GetConfigFilePath());
				return true;
			}
			catch (Exception ex)
			{
				ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
				return false;

			}
		} // End sub

		/// <summary>
		/// Loads an app config file for changing parameters
		/// </summary>
		/// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
		private XmlDocument LoadConfigDocument()
		{
			try
			{
				var MyDoc = new XmlDocument();
				MyDoc.Load(GetConfigFilePath());
				return MyDoc;
			}
			catch (Exception ex)
			{
				ErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
				return null;
			}
		}

		/// <summary>
		/// Specifies the full name and path for the application config file
		/// </summary>
		/// <returns>String containing full name and path</returns>
		private string GetConfigFilePath()
		{
			return Application.ExecutablePath + ".config";
		}

		private string DbCStr(object InpObj)
		{
			if (InpObj == null)
			{
				return "";
			}
			
			return InpObj.ToString();
		}

		private void WriteErrorMsg(string errMsg)
		{
			if (m_MCParamsLoaded)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errMsg);
			}
			else
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, errMsg);
			}
		}
		#endregion
	}	// End class
}	// End namespace
