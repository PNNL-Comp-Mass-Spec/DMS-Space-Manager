//*********************************************************************************************************
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/08/2010
//
//*********************************************************************************************************

namespace Space_Manager
{
    public interface IMgrParams
    {
        //*********************************************************************************************************
        // Defines interface for manager parameter handling
        //**********************************************************************************************************

        string GetParam(string ItemKey);
        void SetParam(string ItemKey, string ItemValue);

    }
}
