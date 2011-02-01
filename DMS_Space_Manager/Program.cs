
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
using System.Windows.Forms;

namespace Space_Manager
{
	static class Program
	{
		//*********************************************************************************************************
		// Application startup class
		//**********************************************************************************************************

		#region "Class variables"
			private static clsMainProgram m_MainProgram;
		#endregion

		#region "Methods"
			/// <summary>
			/// The main entry point for the application.
			/// </summary>
			[STAThread]
			static void Main()
			{
				bool restart = false;
				do
				{
					try
					{
						if (m_MainProgram == null)
						{
							//Initialize the main execution class
							m_MainProgram = new clsMainProgram();
							if (!m_MainProgram.InitMgr())
							{
								return;
							}
						}
						restart = m_MainProgram.PerformSpaceManagement();
						m_MainProgram = null;
					}
					catch (Exception ex)
					{
						string errMsg = "Critical exception starting application";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.FATAL, errMsg, ex);
						return;
					}
					Properties.Settings.Default.Reload();
				} while (restart);
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
