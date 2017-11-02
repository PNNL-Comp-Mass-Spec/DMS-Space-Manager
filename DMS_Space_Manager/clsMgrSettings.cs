﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/08/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Xml;
using System.Windows.Forms;
using PRISM;

namespace Space_Manager
{
    /// <summary>
    /// Class for loading, storing and accessing manager parameters.
    /// </summary>
    /// <remarks>
    /// Loads initial settings from local config file, then checks to see if remainder of settings
    /// should be loaded or manager set to inactive. If manager active, retrieves remainder of settings
    /// from manager control database.
    /// </remarks>
    public class clsMgrSettings : clsLoggerBase, IMgrParams
    {

        #region "Constants"

        /// <summary>
        /// Status message for when the manager is deactivated locally
        /// </summary>
        /// <remarks>Used when MgrActive_Local is False in AnalysisManagerProg.exe.config</remarks>
        public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";

        /// <summary>
        /// Manager parameter: config database connection string
        /// </summary>
        public const string MGR_PARAM_MGR_CFG_DB_CONN_STRING = "MgrCnfgDbConnectStr";
        /// <summary>
        /// Manager parameter: manager active
        /// </summary>
        /// <remarks>Defined in AnalysisManagerProg.exe.config</remarks>
        public const string MGR_PARAM_MGR_ACTIVE_LOCAL = "MgrActive_Local";

        /// <summary>
        /// Manager parameter: manager name
        /// </summary>
        public const string MGR_PARAM_MGR_NAME = "MgrName";

        /// <summary>
        /// Manager parameter: using defaults flag
        /// </summary>
        public const string MGR_PARAM_USING_DEFAULTS = "UsingDefaults";

        /// <summary>
        /// Connection string to DMS5
        /// </summary>
        public const string MGR_PARAM_DEFAULT_DMS_CONN_STRING = "DefaultDMSConnString";

        #endregion

        #region "Class variables"

        private readonly Dictionary<string, string> m_ParamDictionary;
        private bool m_MCParamsLoaded;
        private string m_ErrMsg = "";

        #endregion

        #region "Properties"

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrMsg => m_ErrMsg;

        public Dictionary<string, string> TaskDictionary => m_ParamDictionary;

        #endregion

        #region "Methods"

        public clsMgrSettings()
        {
            m_ParamDictionary = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            if (!LoadSettings())
            {
                if (string.Equals(m_ErrMsg, DEACTIVATED_LOCALLY))
                    throw new ApplicationException(DEACTIVATED_LOCALLY);

                throw new ApplicationException("Unable to initialize manager settings class: " + m_ErrMsg);
            }
        }

        public bool LoadSettings()
        {
            m_ErrMsg = "";

            m_ParamDictionary.Clear();

            // Get settings from config file
            var settingsFromFile = LoadMgrSettingsFromFile();
            foreach (var item in settingsFromFile)
            {
                m_ParamDictionary.Add(item.Key, item.Value);
            }

            // Get directory for main executable
            var appPath = Application.ExecutablePath;
            var fi = new FileInfo(appPath);
            m_ParamDictionary.Add("ApplicationPath", fi.DirectoryName);

            // Test the settings retrieved from the config file
            if (!CheckInitialSettings(m_ParamDictionary))
            {
                // Error logging handled by CheckInitialSettings
                return false;
            }

            // Determine if manager is deactivated locally
            if (!GetBooleanParam(MGR_PARAM_MGR_ACTIVE_LOCAL))
            {
                LogWarning(DEACTIVATED_LOCALLY);
                m_ErrMsg = DEACTIVATED_LOCALLY;
                return false;
            }

            // Get remaining settings from database
            if (!LoadMgrSettingsFromDB())
            {
                // Error logging handled by LoadMgrSettingsFromDB
                return false;
            }

            // Set flag indicating params have been loaded from manger config db
            m_MCParamsLoaded = true;

            // No problems found
            return true;
        }

        private Dictionary<string, string> LoadMgrSettingsFromFile()
        {
            // Load initial settings into string dictionary for return
            var mgrSettingsFromFile = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            // Manager config DB connection string
            var mgrCfgDBConnString = Properties.Settings.Default.MgrCnfgDbConnectStr;
            mgrSettingsFromFile.Add(MGR_PARAM_MGR_CFG_DB_CONN_STRING, mgrCfgDBConnString);

            // Manager active flag
            var mgrActiveLocal = Properties.Settings.Default.MgrActive_Local.ToString();
            mgrSettingsFromFile.Add(MGR_PARAM_MGR_ACTIVE_LOCAL, mgrActiveLocal);

            // Manager name
            // If the MgrName setting in the CaptureTaskManager.exe.config file contains the text $ComputerName$
            // that text is replaced with this computer's domain name
            // This is a case-sensitive comparison
            //
            var mgrName = Properties.Settings.Default.MgrName;
            if (mgrName.Contains("$ComputerName$"))
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, mgrName.Replace("$ComputerName$", Environment.MachineName));
            else
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, mgrName);

            // Default settings in use flag
            var usingDefaults = Properties.Settings.Default.UsingDefaults.ToString();
            mgrSettingsFromFile.Add(MGR_PARAM_USING_DEFAULTS, usingDefaults);

            // Default connection string for logging errors to the database
            // Will get updated later when manager settings are loaded from the manager control database
            var defaultDMSConnectionString = Properties.Settings.Default.DefaultDMSConnString;
            mgrSettingsFromFile.Add(MGR_PARAM_DEFAULT_DMS_CONN_STRING, defaultDMSConnectionString);

            return mgrSettingsFromFile;
        }

        private bool CheckInitialSettings(IReadOnlyDictionary<string, string> InpDict)
        {
            // Verify manager settings dictionary exists
            if (InpDict == null)
            {
                m_ErrMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found";
                LogError(m_ErrMsg, true);
                return false;
            }

            // Verify intact config file was found
            if (!InpDict.TryGetValue(MGR_PARAM_USING_DEFAULTS, out var strValue))
            {
                m_ErrMsg = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file";
                Console.WriteLine(m_ErrMsg);
                LogError(m_ErrMsg, true);
            }
            else
            {

                if (bool.TryParse(strValue, out var blnValue))
                {
                    if (blnValue)
                    {
                        m_ErrMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True";
                        LogError(m_ErrMsg, true);
                        return false;
                    }
                }
            }

            // No problems found
            return true;
        }

        private string GetGroupNameFromSettings(DataTable dtSettings)
        {
            foreach (DataRow oRow in dtSettings.Rows)
            {
                // Add the column heading and value to the dictionary
                var paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);

                if (string.Equals(paramKey, "MgrSettingGroupName", StringComparison.CurrentCultureIgnoreCase))
                {
                    var groupName = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);
                    if (!string.IsNullOrWhiteSpace(groupName))
                    {
                        return groupName;
                    }

                    return string.Empty;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Retrieves the manager settings from the Manager Control database
        /// </summary>
        /// <returns></returns>
        public bool LoadMgrSettingsFromDB(bool logConnectionErrors = true)
        {
            // Requests manager parameters from database. Input string specifies view to use. Performs retries if necessary.

            var managerName = GetParam(MGR_PARAM_MGR_NAME, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                m_ErrMsg =
                    "MgrName parameter not found in m_ParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
                WriteErrorMsg(m_ErrMsg);
                return false;
            }

            var success = LoadMgrSettingsFromDBWork(managerName, out var dtSettings, logConnectionErrors,
                                                    returnErrorIfNoParameters: true);
            if (!success)
            {
                return false;
            }

            success = StoreParameters(dtSettings, skipExistingParameters: false, managerName: managerName);

            if (!success)
                return false;

            while (success)
            {
                var mgrSettingsGroup = GetGroupNameFromSettings(dtSettings);
                if (string.IsNullOrEmpty(mgrSettingsGroup))
                {
                    break;
                }

                // This manager has group-based settings defined; load them now

                success = LoadMgrSettingsFromDBWork(mgrSettingsGroup, out dtSettings, logConnectionErrors,
                                                    returnErrorIfNoParameters: false);

                if (success)
                {
                    success = StoreParameters(dtSettings, skipExistingParameters: true, managerName: managerName);
                }
            }

            return success;
        }

        private bool LoadMgrSettingsFromDBWork(string managerName, out DataTable dtSettings, bool logConnectionErrors,
                                               bool returnErrorIfNoParameters, int retryCount = 3)
        {
            var DBConnectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING, "");
            dtSettings = null;

            if (string.IsNullOrEmpty(DBConnectionString))
            {
                m_ErrMsg = MGR_PARAM_MGR_CFG_DB_CONN_STRING +
                           " parameter not found in m_ParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
                WriteErrorMsg(m_ErrMsg);
                return false;
            }

            var sqlStr = string.Format("SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '{0}'",
                managerName);

            // Get a datatable holding the parameters for this manager
            while (retryCount >= 0)
            {
                try
                {
                    using (var cn = new SqlConnection(DBConnectionString))
                    {
                        var cmd = new SqlCommand
                        {
                            CommandType = CommandType.Text,
                            CommandText = sqlStr,
                            Connection = cn,
                            CommandTimeout = 30
                        };

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            using (var ds = new DataSet())
                            {
                                da.Fill(ds);
                                dtSettings = ds.Tables[0];
                            }
                        }
                    }
                    break;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var msg = string.Format("LoadMgrSettingsFromDB; Exception getting manager settings from database: {0}; " +
                                            "ConnectionString: {1}, RetryCount = {2}",
                                            ex.Message, DBConnectionString, retryCount);

                    if (logConnectionErrors)
                        WriteErrorMsg(msg, allowLogToDB: false);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

            // If loop exited due to errors, return false
            if (retryCount < 0)
            {
                // Log the message to the DB if the monthly Windows updates are not pending
                var allowLogToDB = !(clsWindowsUpdateStatus.ServerUpdatesArePending());

                m_ErrMsg =
                    "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
                if (logConnectionErrors)
                    WriteErrorMsg(m_ErrMsg, allowLogToDB);
                return false;
            }

            // Validate that the data table object is initialized
            if (dtSettings == null)
            {
                // Data table not initialized
                m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; dtSettings datatable is null; using " +
                           DBConnectionString;
                if (logConnectionErrors)
                    WriteErrorMsg(m_ErrMsg);
                return false;
            }

            // Verify at least one row returned
            if (dtSettings.Rows.Count < 1 && returnErrorIfNoParameters)
            {
                // Wrong number of rows returned
                m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Manager " + managerName +
                           " not defined in the manager control database; using " + DBConnectionString;
                WriteErrorMsg(m_ErrMsg);
                dtSettings.Dispose();
                return false;
            }

            return true;
        }

        /// <summary>
        /// Update mParamDictionary with settings in dtSettings, optionally skipping existing parameters
        /// </summary>
        /// <param name="dtSettings"></param>
        /// <param name="skipExistingParameters"></param>
        /// <param name="managerName"></param>
        /// <returns></returns>
        private bool StoreParameters(DataTable dtSettings, bool skipExistingParameters, string managerName)
        {
            bool success;

            try
            {
                foreach (DataRow oRow in dtSettings.Rows)
                {
                    // Add the column heading and value to the dictionary
                    var paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);
                    var paramVal = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);

                    if (paramKey.ToLower() == "perspective" && Environment.MachineName.ToLower().StartsWith("monroe"))
                    {
                        if (paramVal.ToLower() == "server")
                        {
                            paramVal = "client";
                            Console.WriteLine(
                                @"StoreParameters: Overriding manager perspective to be 'client' because impersonating a server-based manager from an office computer");
                        }
                    }

                    if (m_ParamDictionary.ContainsKey(paramKey))
                    {
                        if (!skipExistingParameters)
                        {
                            m_ParamDictionary[paramKey] = paramVal;
                        }
                    }
                    else
                    {
                        m_ParamDictionary.Add(paramKey, paramVal);
                    }
                }
                success = true;
            }
            catch (Exception ex)
            {
                m_ErrMsg =
                    "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table for manager '" +
                    managerName + "': " + ex.Message;
                WriteErrorMsg(m_ErrMsg);
                success = false;
            }
            finally
            {
                dtSettings?.Dispose();
            }

            return success;
        }

        /// <summary>
        /// Lookup the value of a boolean parameter
        /// </summary>
        /// <param name="itemKey"></param>
        /// <returns>True/false for the given parameter; false if the parameter is not present</returns>
        public bool GetBooleanParam(string itemKey)
        {
            var itemValue = GetParam(itemKey, string.Empty);

            if (string.IsNullOrWhiteSpace(itemValue))
                return false;

            if (bool.TryParse(itemValue, out var itemBool))
                return itemBool;

            return false;
        }

        public string GetParam(string itemKey)
        {
            return GetParam(itemKey, string.Empty);
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public string GetParam(string itemKey, string valueIfMissing)
        {
            if (m_ParamDictionary.TryGetValue(itemKey, out var itemValue))
            {
                return itemValue ?? string.Empty;
            }

            return valueIfMissing ?? string.Empty;
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public bool GetParam(string itemKey, bool valueIfMissing)
        {
            if (m_ParamDictionary.TryGetValue(itemKey, out var valueText))
            {
                var value = clsConversion.CBoolSafe(valueText, valueIfMissing);
                return value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public int GetParam(string itemKey, int valueIfMissing)
        {
            if (m_ParamDictionary.TryGetValue(itemKey, out var valueText))
            {
                var value = clsConversion.CIntSafe(valueText, valueIfMissing);
                return value;
            }

            return valueIfMissing;
        }

        public void SetParam(string itemKey, string itemValue)
        {
            if (m_ParamDictionary.ContainsKey(itemKey))
            {
                m_ParamDictionary[itemKey] = itemValue;
            }
            else
            {
                m_ParamDictionary.Add(itemKey, itemValue);
            }
        }

        /// <summary>
        /// Writes specfied value to an application config file.
        /// </summary>
        /// <param name="key">Name for parameter (case sensitive)</param>
        /// <param name="value">New value for parameter</param>
        /// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
        /// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
        public bool WriteConfigSetting(string key, string value)
        {
            m_ErrMsg = "";

            // Load the config document
            var doc = LoadConfigDocument();
            if (doc == null)
            {
                // Error message has already been produced by LoadConfigDocument
                return false;
            }

            // Retrieve the settings node
            var appSettingsNode = doc.SelectSingleNode("//applicationSettings");

            if (appSettingsNode == null)
            {
                m_ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
                return false;
            }

            try
            {
                // Select the element containing the value for the specified key containing the key
                var matchingElement = (XmlElement)appSettingsNode.SelectSingleNode(string.Format("//setting[@name='{0}']/value", key));
                if (matchingElement != null)
                {
                    // Set key to specified value
                    matchingElement.InnerText = value;
                }
                else
                {
                    // Key was not found
                    m_ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                doc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Loads an app config file for changing parameters
        /// </summary>
        /// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
        private XmlDocument LoadConfigDocument()
        {
            try
            {
                var doc = new XmlDocument();
                doc.Load(GetConfigFilePath());
                return doc;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
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

        private void WriteErrorMsg(string errorMessage, bool allowLogToDB = true)
        {
            var logToDb = !m_MCParamsLoaded && allowLogToDB;
            LogError(errorMessage, logToDb);
        }

        #endregion
    }
}
