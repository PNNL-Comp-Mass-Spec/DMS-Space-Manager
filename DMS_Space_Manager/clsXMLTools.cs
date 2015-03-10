﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************
using System;
using System.Xml;

namespace Space_Manager
{
    class clsXMLTools
    {
        //*********************************************************************************************************
        // Tools for parsing input XML
        //**********************************************************************************************************

        #region "Methods"

        /// <summary>
        /// Converts broadcast XML string into a dictionary of strings
        /// </summary>
        /// <param name="InputXML">XML string to parse</param>
        /// <returns>String dictionary of broadcast sections</returns>
        public static clsBroadcastCmd ParseBroadcastXML(string InputXML)
        {
            var returnedData = new clsBroadcastCmd();

            try
            {
                var doc = new XmlDocument();
                doc.LoadXml(InputXML);

                // Get list of managers this command applies to
                foreach (XmlNode xn in doc.SelectNodes("//Managers/*"))
                {
                    returnedData.MachineList.Add(xn.InnerText);
                }

                // Get command contained in message
                returnedData.MachCmd = doc.SelectSingleNode("//Message").InnerText;

                // Return the parsing results
                return returnedData;
            }
            catch (Exception Ex)
            {
                throw new Exception("Exception while parsing broadcast string", Ex);
            }
        }
        #endregion
    }
}