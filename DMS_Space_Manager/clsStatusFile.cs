﻿
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/08/2010
//
//*********************************************************************************************************
using System;
using System.Xml;
using System.IO;

namespace Space_Manager
{
    public class clsStatusFile : IStatusFile
    {
        //*********************************************************************************************************
        // Class to handle status file updates
        //**********************************************************************************************************

        #region "Class variables"
        //Status file name and location
        private string m_FileNamePath = "";

        //Manager name
        private string m_MgrName = "";

        //Status value
        private EnumMgrStatus m_MgrStatus = EnumMgrStatus.Stopped;

        //Manager start time
        private DateTime m_MgrStartTime;

        //CPU utilization
        private int m_CpuUtilization;

        //Analysis Tool
        private string m_Tool = "";

        //Task status
        private EnumTaskStatus m_TaskStatus = EnumTaskStatus.No_Task;

        //Task duration
        private float m_Duration;

        //Progess (value between 0 and 100)
        private float m_Progress;

        //Current operation (freeform string)
        private string m_CurrentOperation = "";

        //Task status detail
        private EnumTaskStatusDetail m_TaskStatusDetail = EnumTaskStatusDetail.No_Task;

        //Job number
        private int m_JobNumber;

        //Job step
        private int m_JobStep;

        //Dataset name
        private string m_Dataset = "";

        //Most recent job info
        private string m_MostRecentJobInfo = "";

        //Number of spectrum files created
        private int m_SpectrumCount;

        //Message broker connection string
        private string m_MessageQueueURI;

        //Broker topic for status reporting
        private string m_MessageQueueTopic;

        //Flag to indicate if status should be logged to broker in addition to a file
        private bool m_LogToMsgQueue;
        #endregion

        #region "Properties"
        public string FileNamePath
        {
            get { return m_FileNamePath; }
            set { m_FileNamePath = value; }
        }

        public string MgrName
        {
            get { return m_MgrName; }
            set { m_MgrName = value; }
        }

        public EnumMgrStatus MgrStatus
        {
            get { return m_MgrStatus; }
            set { m_MgrStatus = value; }
        }

        public int CpuUtilization
        {
            get { return m_CpuUtilization; }
            set { m_CpuUtilization = value; }
        }

        public string Tool
        {
            get { return m_Tool; }
            set { m_Tool = value; }
        }

        public EnumTaskStatus TaskStatus
        {
            get { return m_TaskStatus; }
            set { m_TaskStatus = value; }
        }

        public float Duration
        {
            get { return m_Duration; }
            set { m_Duration = value; }
        }

        public float Progress
        {
            get { return m_Progress; }
            set { m_Progress = value; }
        }

        public string CurrentOperation
        {
            get { return m_CurrentOperation; }
            set { m_CurrentOperation = value; }
        }

        public EnumTaskStatusDetail TaskStatusDetail
        {
            get { return m_TaskStatusDetail; }
            set { m_TaskStatusDetail = value; }
        }

        public int JobNumber
        {
            get { return m_JobNumber; }
            set { m_JobNumber = value; }
        }

        public int JobStep
        {
            get { return m_JobStep; }
            set { m_JobStep = value; }
        }

        public string Dataset
        {
            get { return m_Dataset; }
            set { m_Dataset = value; }
        }

        public string MostRecentJobInfo
        {
            get { return m_MostRecentJobInfo; }
            set { m_MostRecentJobInfo = value; }
        }

        public int SpectrumCount
        {
            get { return m_SpectrumCount; }
            set { m_SpectrumCount = value; }
        }

        public string MessageQueueURI
        {
            get { return m_MessageQueueURI; }
            set { m_MessageQueueURI = value; }
        }

        public string MessageQueueTopic
        {
            get { return m_MessageQueueTopic; }
            set { m_MessageQueueTopic = value; }
        }

        public bool LogToMsgQueue
        {
            get { return m_LogToMsgQueue; }
            set { m_LogToMsgQueue = value; }
        }
        #endregion

        #region "Constructors"
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="FileLocation">Full path to status file</param>
        public clsStatusFile(string FileLocation)
        {
            m_FileNamePath = FileLocation;
            m_MgrStartTime = DateTime.Now;
            m_Progress = 0;
            m_SpectrumCount = 0;
            m_Dataset = "";
            m_JobNumber = 0;
            m_Tool = "";
        }
        #endregion

        #region "Events"
        public event StatusMonitorUpdateReceived MonitorUpdateRequired;
        #endregion

        #region "Methods"
        /// <summary>
        /// Clears cached status info
        /// </summary>
        public void ClearCachedInfo()
        {
            m_Progress = 0;
            m_SpectrumCount = 0;
            m_Dataset = "";
            m_JobNumber = 0;
            m_JobStep = 0;
            m_Tool = "";
            m_Duration = 0;

            // Only clear the recent job info if the variable is Nothing
            if (m_MostRecentJobInfo == null)
            {
                m_MostRecentJobInfo = string.Empty;
            }

        }

        /// <summary>
        /// Converts the manager status enum to a string value
        /// </summary>
        /// <param name="StatusEnum">An IStatusFile.EnumMgrStatus object</param>
        /// <returns>String representation of input object</returns>
        private string ConvertMgrStatusToString(EnumMgrStatus StatusEnum)
        {
            return StatusEnum.ToString("G");
        }

        /// <summary>
        /// Converts the task status enum to a string value
        /// </summary>
        /// <param name="StatusEnum">An IStatusFile.EnumTaskStatus object</param>
        /// <returns>String representation of input object</returns>
        private string ConvertTaskStatusToString(EnumTaskStatus StatusEnum)
        {
            return StatusEnum.ToString("G");
        }

        /// <summary>
        /// Converts the manager status enum to a string value
        /// </summary>
        /// <param name="StatusEnum">An IStatusFile.EnumTaskStatusDetail object</param>
        /// <returns>String representation of input object</returns>
        private string ConvertTaskDetailStatusToString(EnumTaskStatusDetail StatusEnum)
        {
            return StatusEnum.ToString("G");
        }

        /// <summary>
        /// Writes the status file
        /// </summary>
        public void WriteStatusFile()
        {
            string XMLText = string.Empty;

            //Set up the XML writer
            try
            {
                //Create a memory stream to write the document in
                var MemStream = new MemoryStream();
                using (var XWriter = new XmlTextWriter(MemStream, System.Text.Encoding.UTF8))
                {
                    XWriter.Formatting = Formatting.Indented;
                    XWriter.Indentation = 2;

                    //Write the file
                    XWriter.WriteStartDocument(true);
                    //Root level element
                    XWriter.WriteStartElement("Root");
                    XWriter.WriteStartElement("Manager");
                    XWriter.WriteElementString("MgrName", m_MgrName);
                    XWriter.WriteElementString("MgrStatus", ConvertMgrStatusToString(m_MgrStatus));
                    XWriter.WriteElementString("LastUpdate", DateTime.Now.ToString());
                    XWriter.WriteElementString("LastStartTime", m_MgrStartTime.ToString());
                    XWriter.WriteElementString("CPUUtilization", m_CpuUtilization.ToString());
                    XWriter.WriteElementString("FreeMemoryMB", "0");
                    XWriter.WriteStartElement("RecentErrorMessages");
                    foreach (string ErrMsg in clsStatusData.ErrorQueue)
                    {
                        XWriter.WriteElementString("ErrMsg", ErrMsg);
                    }
                    XWriter.WriteEndElement();
                    //Error messages
                    XWriter.WriteEndElement();
                    //Manager section

                    XWriter.WriteStartElement("Task");
                    XWriter.WriteElementString("Tool", m_Tool);
                    XWriter.WriteElementString("Status", ConvertTaskStatusToString(m_TaskStatus));
                    XWriter.WriteElementString("Duration", m_Duration.ToString("##0.0"));
                    XWriter.WriteElementString("DurationMinutes", (60f * m_Duration).ToString("##0.0"));
                    XWriter.WriteElementString("Progress", m_Progress.ToString("##0.00"));
                    XWriter.WriteElementString("CurrentOperation", m_CurrentOperation);
                    XWriter.WriteStartElement("TaskDetails");
                    XWriter.WriteElementString("Status", ConvertTaskDetailStatusToString(m_TaskStatusDetail));
                    XWriter.WriteElementString("Job", m_JobNumber.ToString());
                    XWriter.WriteElementString("Step", m_JobStep.ToString());
                    XWriter.WriteElementString("Dataset", m_Dataset);
                    XWriter.WriteElementString("MostRecentLogMessage", clsStatusData.MostRecentLogMessage);
                    XWriter.WriteElementString("MostRecentJobInfo", m_MostRecentJobInfo);
                    XWriter.WriteElementString("SpectrumCount", m_SpectrumCount.ToString());
                    XWriter.WriteEndElement();	//Task details section
                    XWriter.WriteEndElement();	//Task section
                    XWriter.WriteEndElement();	//Root section

                    //Close the document, but don't close the writer yet
                    XWriter.WriteEndDocument();
                    XWriter.Flush();

                    //Use a streamreader to copy the XML text to a string variable
                    MemStream.Seek(0, SeekOrigin.Begin);
                    var MemStreamReader = new StreamReader(MemStream);
                    XMLText = MemStreamReader.ReadToEnd();

                    MemStreamReader.Close();
                    MemStream.Close();

                    //  Since strXMLText now contains the XML, we can now safely close XWriter
                }

                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                //Write the output file
                try
                {
                    using (var OutFile = new StreamWriter(new FileStream(m_FileNamePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        OutFile.WriteLine(XMLText);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error writing status file: " + ex.Message);
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            //Log to a message queue
            if (m_LogToMsgQueue) LogStatusToMessageQueue(XMLText);
        }

        /// <summary>
        /// Writes the status to the message queue
        /// </summary>
        /// <param name="strStatusXML">A string contiaining the XML to write</param>
        protected void LogStatusToMessageQueue(string strStatusXML)
        {
            if (MonitorUpdateRequired != null) MonitorUpdateRequired(strStatusXML);

        }

        /// <summary>
        /// Updates status file
        /// (Overload to update when completion percentage is only change)
        /// </summary>
        /// <param name="PercentComplete">Job completion percentage (between 0 and 100)</param>
        public void UpdateAndWrite(float PercentComplete)
        {
            m_Progress = PercentComplete;

            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file
        /// (Overload to update file when status and completion percentage change)
        /// </summary>
        /// <param name="Status">Job status enum</param>
        /// <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
        public void UpdateAndWrite(EnumTaskStatusDetail Status, float PercentComplete)
        {
            m_TaskStatusDetail = Status;
            m_Progress = PercentComplete;

            WriteStatusFile();
        }

        /// <summary>
        /// Sets status file to show mahager not running
        /// </summary>
        /// <param name="MgrError">TRUE if manager not running due to error; FALSE otherwise</param>
        public void UpdateStopped(bool MgrError)
        {
            ClearCachedInfo();

            if (MgrError)
            {
                m_MgrStatus = EnumMgrStatus.Stopped_Error;
            }
            else
            {
                m_MgrStatus = EnumMgrStatus.Stopped;
            }

            m_TaskStatus = EnumTaskStatus.No_Task;
            m_TaskStatusDetail = EnumTaskStatusDetail.No_Task;

            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file to show manager disabled
        /// </summary>
        /// <param name="Local">TRUE if manager disabled locally, otherwise FALSE</param>
        public void UpdateDisabled(bool Local)
        {
            ClearCachedInfo();

            if (Local)
            {
                m_MgrStatus = EnumMgrStatus.Disabled_Local;
            }
            else
            {
                m_MgrStatus = EnumMgrStatus.Disabled_MC;
            }

            m_TaskStatus = EnumTaskStatus.No_Task;
            m_TaskStatusDetail = EnumTaskStatusDetail.No_Task;

            this.WriteStatusFile();
        }

        /// <summary>
        /// Updates status file to show manager in idle state
        /// </summary>
        public void UpdateIdle()
        {
            ClearCachedInfo();

            m_MgrStatus = EnumMgrStatus.Running;
            m_TaskStatus = EnumTaskStatus.No_Task;
            m_TaskStatusDetail = EnumTaskStatusDetail.No_Task;

            WriteStatusFile();
        }

        /// <summary>
        /// Initializes the status from a file, if file exists
        /// </summary>
        /// 
        public void InitStatusFromFile()
        {
            //Verify status file exists
            if (!File.Exists(m_FileNamePath)) return;

            //Get data from status file
            try
            {
                // Read the input file
                string XmlStr = File.ReadAllText(m_FileNamePath);

                // Convert to an XML document
                var Doc = new XmlDocument();
                Doc.LoadXml(XmlStr);

                // Get the most recent log message
                clsStatusData.MostRecentLogMessage = Doc.SelectSingleNode(@"//Task/TaskDetails/MostRecentLogMessage").InnerText;

                //Get the most recent job info
                m_MostRecentJobInfo = Doc.SelectSingleNode(@"//Task/TaskDetails/MostRecentJobInfo").InnerText;

                //Get the error messsages
                foreach (XmlNode Xn in Doc.SelectNodes(@"//Manager/RecentErrorMessages/ErrMsg"))
                {
                    clsStatusData.AddErrorMessage(Xn.InnerText);
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception reading status file", ex);
            }

        }
        #endregion

    }	// End class
}	// End namespace
