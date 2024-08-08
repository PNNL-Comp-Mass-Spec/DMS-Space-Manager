//*********************************************************************************************************
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
    internal static class StatusData
    {
        private static string mMostRecentLogMessage;

        private static readonly Queue<string> mErrorQueue = new();

        public static string MostRecentLogMessage
        {
            get => mMostRecentLogMessage;
            set
            {
                //Filter out routine startup and shutdown messages
                if (value.Contains("=== Started") || value.Contains("===== Closing"))
                {
                    //Do nothing
                }
                else
                {
                    mMostRecentLogMessage = value;
                }
            }
        }

        public static IEnumerable<string> ErrorQueue => mErrorQueue;

        public static void AddErrorMessage(string errorMsg)
        {
            //Add the most recent error message
            mErrorQueue.Enqueue(errorMsg);

            // If there are more than 4 entries in the queue, delete the oldest ones
            while (mErrorQueue.Count > 4)
            {
                mErrorQueue.Dequeue();
            }
        }
    }
}
