//*********************************************************************************************************
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
            var appPath = Application.ExecutablePath;
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
            if (!LoadMgrSettingsFromDB(m_ParamDictionary))
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
            var TempStr = Properties.Settings.Default.MgrCnfgDbConnectStr;
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

        private bool CheckInitialSettings(IReadOnlyDictionary<string, string> settingsDict)
        {
            //Verify manager settings dictionary exists
            if (settingsDict == null)
            {
                var msg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }

            //Verify intact config file was found
            if (bool.Parse(settingsDict["UsingDefaults"]))
            {
                var msg = "clsMgrSettings.CheckInitialSettings(); Config file problem, default settings being used";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }

            //No problems found
            return true;
        }

        public bool LoadMgrSettingsFromDB()
        {
            return LoadMgrSettingsFromDB(m_ParamDictionary);
        }

        public bool LoadMgrSettingsFromDB(Dictionary<string, string> mgrSettingsDict)
        {
            //Requests manager parameters from database. Input string specifies view to use. Performs retries if necessary.
            short retryCount = 3;
            string myMsg;

            var SqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + mgrSettingsDict["MgrName"] + "'";

            //Get a table containing data for job
            DataTable resultsTable = null;

            //Get a datatable holding the parameters for one manager
            while (retryCount > 0)
            {
                try
                {
                    using (var Cn = new SqlConnection(mgrSettingsDict["MgrCnfgDbConnectStr"]))
                    {
                        using (var Da = new SqlDataAdapter(SqlStr, Cn))
                        {
                            using (var Ds = new DataSet())
                            {
                                Da.Fill(Ds);
                                resultsTable = Ds.Tables[0];
                            }
                        }
                    }

                    break;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    myMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " + ex.Message;
                    myMsg = myMsg + ", RetryCount = " + retryCount;
                    WriteErrorMsg(myMsg);
                    //Delay for 5 seconds before trying again
                    System.Threading.Thread.Sleep(5000);
                }
            }

            //If loop exited due to errors, return false
            if (retryCount < 1)
            {
                myMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
                WriteErrorMsg(myMsg);
                resultsTable?.Dispose();
                return false;
            }

            if (resultsTable == null)
            {
                return false;
            }

            //Verify at least one row returned
            if (resultsTable.Rows.Count < 1)
            {
                //Wrong number of rows returned
                myMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Invalid row count retrieving manager settings: RowCount = ";
                myMsg += resultsTable.Rows.Count.ToString(CultureInfo.InvariantCulture);
                WriteErrorMsg(myMsg);
                resultsTable.Dispose();
                return false;
            }

            //Fill a string dictionary with the manager parameters that have been found
            try
            {
                foreach (DataRow resultRow in resultsTable.Rows)
                {
                    //Add the column heading and value to the dictionary
                    var ParamKey = DbCStr(resultRow[resultsTable.Columns["ParameterName"]]);
                    var ParamVal = DbCStr(resultRow[resultsTable.Columns["ParameterValue"]]);
                    if (mgrSettingsDict.ContainsKey(ParamKey))
                    {
                        mgrSettingsDict[ParamKey] = ParamVal;
                    }
                    else
                    {
                        mgrSettingsDict.Add(ParamKey, ParamVal);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                myMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " + ex.Message;
                WriteErrorMsg(myMsg);
                return false;
            }
            finally
            {
                resultsTable.Dispose();
            }
        }

        public string GetParam(string itemKey)
        {
            string retStr;
            if (m_ParamDictionary.TryGetValue(itemKey, out retStr))
                return retStr;

            return string.Empty;
        }

        public void SetParam(string itemKey, string itemValue)
        {
            m_ParamDictionary[itemKey] = itemValue;
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

            ErrMsg = "";

            //Load the config document
            var doc = LoadConfigDocument();
            if (doc == null)
            {
                //Error message has already been produced by LoadConfigDocument
                return false;
            }

            //Retrieve the settings node
            var MyNode = doc.SelectSingleNode("//applicationSettings");

            if (MyNode == null)
            {
                ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
                return false;
            }

            try
            {
                //Select the element containing the value for the specified key containing the key
                var MyElement = (XmlElement)MyNode.SelectSingleNode(string.Format("//setting[@name='{0}']/value", key));
                if (MyElement != null)
                {
                    //Set key to specified value
                    MyElement.InnerText = value;
                }
                else
                {
                    //key was not found
                    ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                doc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
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
    }
}
