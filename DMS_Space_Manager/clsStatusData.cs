﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/08/2010
//
//*********************************************************************************************************

using System.Collections.Generic;

namespace Space_Manager
{
    /// <summary>
    /// Class to hold long-term data for status reporting. This is a hack to avoid adding an instance of the
    ///	status file class to the log tools class
    /// </summary>
    internal static class clsStatusData
    {
        private static string m_MostRecentLogMessage;

        private static readonly Queue<string> m_ErrorQueue = new();

        public static string MostRecentLogMessage
        {
            get => m_MostRecentLogMessage;
            set
            {
                //Filter out routine startup and shutdown messages
                if (value.Contains("=== Started") || value.Contains("===== Closing"))
                {
                    //Do nothing
                }
                else
                {
                    m_MostRecentLogMessage = value;
                }
            }
        }

        public static IEnumerable<string> ErrorQueue => m_ErrorQueue;

        public static void AddErrorMessage(string ErrMsg)
        {
            //Add the most recent error message
            m_ErrorQueue.Enqueue(ErrMsg);

            // If there are more than 4 entries in the queue, delete the oldest ones
            while (m_ErrorQueue.Count > 4)
            {
                m_ErrorQueue.Dequeue();
            }
        }
    }
}
