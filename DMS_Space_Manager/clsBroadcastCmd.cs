//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;

namespace Space_Manager
{
    /// <summary>
    /// Class to hold data receieved from Broadcast command queue for control of manager
    /// </summary>
    [Obsolete("Unused")]
    class clsBroadcastCmd
    {

        #region "Class variables"

        #endregion

        #region "Properties"
        /// <summary>
        /// List of machines the received message applies to
        /// </summary>
        public List<string> MachineList { get; set; } = new List<string>();

        // The command that was broadcast
        public string MachCmd { get; set; }
        #endregion
    }
}
