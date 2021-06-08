//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************

using System.Collections.Generic;

namespace Space_Manager
{
    /// <summary>
    /// Interface for space management task parameters
    /// </summary>
    public interface ITaskParams
    {
        Dictionary<string, string> TaskDictionary { get; }

        string GetParam(string name);
        bool AddAdditionalParameter(string paramName, string paramValue);
        void SetParam(string keyName, string value);
    }
}
