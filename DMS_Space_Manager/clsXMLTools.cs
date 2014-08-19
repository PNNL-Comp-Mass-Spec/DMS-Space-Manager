//*********************************************************************************************************
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
	static class clsXMLTools
	{
		//*********************************************************************************************************
		// Tools for parsing input XML
		//**********************************************************************************************************

		#region "Methods"

		/*
		 * Unused function
		 * 
		/// <summary>
		/// Converts command XML string into a dictionary of strings (future)
		/// </summary>
		/// <param name="InputXML">XML string to parse</param>
		/// <returns>String dictionary of command sections</returns>
		[Obsolete("Function not used")]
		public static System.Collections.Specialized.StringDictionary ParseCommandXML(string InputXML)
		{
			var returnDict = new System.Collections.Specialized.StringDictionary();

			XmlDocument doc = new XmlDocument();
			doc.LoadXml(InputXML);

			try
			{
				returnDict.Add("package", doc.SelectSingleNode("//package").InnerText);
				returnDict.Add("local", doc.SelectSingleNode("//local").InnerText);
				returnDict.Add("share", doc.SelectSingleNode("//share").InnerText);
				returnDict.Add("year", doc.SelectSingleNode("//year").InnerText);
				returnDict.Add("team", doc.SelectSingleNode("//team").InnerText);
				returnDict.Add("folder", doc.SelectSingleNode("//folder").InnerText);
				returnDict.Add("cmd", doc.SelectSingleNode("//cmd").InnerText);

				return returnDict;
			}
			catch (Exception Ex)
			{
				throw new Exception("", Ex);	// Message parameter left blank because it is handled at higher level
			}
		}
		*/

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
	}	// End class
}	// End namespace
