﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/14/2010
//
//*********************************************************************************************************

namespace Space_Manager
{
    /// <summary>
    /// Class to hold data for each drive being managed
    /// </summary>
    public class clsDriveData
    {

        #region "Class variables"

        string m_DriveLetter = "";

        #endregion

        #region "Properties"

        public string DriveLetter
        {
            get => AppendColonToDriveLetter(m_DriveLetter);
            set => m_DriveLetter = value;
        }

        public double MinDriveSpace { get; set; }   // Minimum allowable space in GB

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="driveLetter">Drive letter</param>
        /// <param name="driveSpace">Min allowable drive space</param>
        public clsDriveData(string driveLetter, double driveSpace)
        {
            m_DriveLetter = driveLetter;
            MinDriveSpace = driveSpace;
        }
        #endregion

        #region "Methods"
        /// <summary>
        /// Appends colon to drive letter if not already present
        /// </summary>
        /// <param name="inpDrive">Drive letter to test</param>
        /// <returns>Drive letter with appended colon</returns>
        private string AppendColonToDriveLetter(string inpDrive)
        {
            if (!inpDrive.Contains(":"))
                return inpDrive + ":";
            else
                return inpDrive;
        }

        public override string ToString()
        {
            return DriveLetter;
        }
        #endregion

    }
}
