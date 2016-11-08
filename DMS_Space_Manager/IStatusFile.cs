﻿
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/08/2010
//
// Last modified 09/08/2010
//*********************************************************************************************************
using System;

namespace Space_Manager
{
	public interface IStatusFile
	{
		//*********************************************************************************************************
		// Interface used by classes that create and update task status file
		//**********************************************************************************************************

		#region "Events"
			event StatusMonitorUpdateReceived MonitorUpdateRequired;
		#endregion

		#region "Properties"
			string FileNamePath { get;set; }
			string MgrName { get; set; }
			EnumMgrStatus MgrStatus { get; set; }
			int CpuUtilization { get; set; }
			string Tool { get; set; }
			EnumTaskStatus TaskStatus { get; set; }
			float Duration { get; set; }
			float Progress { get; set; }
			string CurrentOperation { get; set; }
			EnumTaskStatusDetail TaskStatusDetail { get; set; }
			int JobNumber { get; set; }
			int JobStep { get; set; }
			string Dataset { get; set; }
			string MostRecentJobInfo { get; set; }
			int SpectrumCount { get; set; }
			bool LogToMsgQueue { get; set; }
			string MessageQueueURI { get; set; }
			string MessageQueueTopic { get; set; }
		#endregion

		#region "Methods"
			void WriteStatusFile();
			void UpdateAndWrite(float PercentComplete);
			void UpdateAndWrite(EnumTaskStatusDetail Status, float PercentComplete);
			void UpdateStopped(bool MgrError);
			void UpdateDisabled(bool Local);
			void UpdateIdle();
			void InitStatusFromFile();
		#endregion
	}
}
