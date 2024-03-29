﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/08/2010
//
//*********************************************************************************************************

using System;

// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedMemberInSuper.Global

namespace Space_Manager
{
    /// <summary>
    /// Interface used by classes that create and update task status file
    /// </summary>
    public interface IStatusFile
    {
        // Ignore Spelling: tcp

        event StatusMonitorUpdateReceived MonitorUpdateRequired;

        /// <summary>
        /// Status file path
        /// </summary>
        string FileNamePath { get; set; }

        /// <summary>
        /// Manager name
        /// </summary>
        string MgrName { get; set; }

        /// <summary>
        /// Manager status
        /// </summary>
        EnumMgrStatus MgrStatus { get; set; }

        /// <summary>
        /// Overall CPU utilization of all threads
        /// </summary>
        int CpuUtilization { get; set; }

        /// <summary>
        /// Step tool name
        /// </summary>
        string Tool { get; set; }

        /// <summary>
        /// Task status
        /// </summary>
        EnumTaskStatus TaskStatus { get; set; }

        /// <summary>
        /// Task start time (UTC-based)
        /// </summary>
        DateTime TaskStartTime { get; set; }

        /// <summary>
        /// Progress (value between 0 and 100)
        /// </summary>
        float Progress { get; set; }

        /// <summary>
        /// Current task
        /// </summary>
        string CurrentOperation { get; set; }

        /// <summary>
        /// Task status detail
        /// </summary>
        EnumTaskStatusDetail TaskStatusDetail { get; set; }

        /// <summary>
        /// Job number
        /// </summary>
        int JobNumber { get; set; }

        /// <summary>
        /// Step number
        /// </summary>
        int JobStep { get; set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        string Dataset { get; set; }

        /// <summary>
        /// Most recent job info
        /// </summary>
        string MostRecentJobInfo { get; set; }

        /// <summary>
        /// URI for the manager status message queue, e.g. tcp://Proto-7.pnl.gov:61616
        /// </summary>
        string MessageQueueURI { get; }

        /// <summary>
        /// Topic name for the manager status message queue
        /// </summary>
        string MessageQueueTopic { get; }

        /// <summary>
        /// When true, the status XML is being sent to the manager status message queue
        /// </summary>
        bool LogToMsgQueue { get; }

        void ClearCachedInfo();

        void ConfigureMessageQueueLogging(bool logStatusToMessageQueue, string msgQueueURI, string messageQueueTopicMgrStatus);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        void UpdateAndWrite(float percentComplete);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        void UpdateAndWrite(EnumTaskStatusDetail status, float percentComplete);

        void UpdateStopped(bool mgrError);

        void UpdateDisabled(bool disabledLocally);

        void UpdateIdle();

        /// <summary>
        /// Writes out a new status file, indicating that the manager is still alive
        /// </summary>
        void WriteStatusFile();
    }
}
