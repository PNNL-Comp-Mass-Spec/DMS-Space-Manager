//*********************************************************************************************************
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
    public class DriveData
    {
        private readonly string m_DriveLetter;

        public string DriveLetter => AppendColonToDriveLetter(m_DriveLetter);

        /// <summary>
        /// Minimum allowable space, in GB
        /// </summary>
        public double MinDriveSpace { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="driveLetter">Drive letter</param>
        /// <param name="driveSpace">Min allowable drive space</param>
        public DriveData(string driveLetter, double driveSpace)
        {
            m_DriveLetter = driveLetter;
            MinDriveSpace = driveSpace;
        }

        /// <summary>
        /// Appends colon to drive letter if not already present
        /// </summary>
        /// <param name="inpDrive">Drive letter to test</param>
        /// <returns>Drive letter with appended colon</returns>
        private string AppendColonToDriveLetter(string inpDrive)
        {
            if (!inpDrive.Contains(":"))
                return inpDrive + ":";

            return inpDrive;
        }

        public override string ToString()
        {
            return DriveLetter;
        }
    }
}
