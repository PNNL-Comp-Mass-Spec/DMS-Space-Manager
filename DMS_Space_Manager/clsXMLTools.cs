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
    [Obsolete("Unused")]
    internal static class clsXMLTools
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
                var managerNodes = doc.SelectNodes("//Managers/*");
                if (managerNodes != null)
                    foreach (XmlNode xn in managerNodes)
                    {
                        returnedData.MachineList.Add(xn.InnerText);
                    }

                // Get command contained in message
                var messageNode = doc.SelectSingleNode("//Message");
                if (messageNode != null)
                    returnedData.MachCmd = messageNode.InnerText;

                // Return the parsing results
                return returnedData;
            }
            catch (Exception ex)
            {
                throw new Exception("Exception while parsing broadcast string", ex);
            }
        }
        #endregion
    }
}