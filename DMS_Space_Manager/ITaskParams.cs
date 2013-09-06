//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
// Last modified 09/09/2010
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Space_Manager
{
	public interface ITaskParams
	{
		//*********************************************************************************************************
		// Interface for space management task parameters
		//**********************************************************************************************************

		#region "Properties"
			Dictionary<string, string> TaskDictionary { get; }
		#endregion

		#region "Methods"
			string GetParam(string name);
			bool AddAdditionalParameter(string paramName, string paramValue);
			void SetParam(string keyName, string value);
		#endregion
	}	// End interface
}	// End namespace
