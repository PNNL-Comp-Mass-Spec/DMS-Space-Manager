
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************
using System;
using PRISM;

namespace Space_Manager
{
    static class Program
    {
        //*********************************************************************************************************
        // Application startup class
        //**********************************************************************************************************

        private const string PROGRAM_DATE = "July 20, 2017";

        private static clsMainProgram m_MainProgram;

        private static bool mPreviewMode;

        private static bool mTraceMode;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            bool restart;

            mPreviewMode = false;
            mTraceMode = false;

            var objParseCommandLine = new clsParseCommandLine();

            // Look for /T or /Test on the command line
            // If present, this means "code test mode" is enabled
            if (objParseCommandLine.ParseCommandLine())
            {
                SetOptionsUsingCommandLineParameters(objParseCommandLine);
            }

            if (objParseCommandLine.NeedToShowHelp)
            {
                ShowProgramHelp();
                return;
            }

            do
            {
                try
                {
                    if (m_MainProgram == null)
                    {
                        //Initialize the main execution class
                        m_MainProgram = new clsMainProgram(mPreviewMode, mTraceMode);

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
                    const string errMsg = "Critical exception starting application";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.FATAL, errMsg, ex);
                    return;
                }
                Properties.Settings.Default.Reload();
            } while (restart);
        }


        private static void SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false

            var strValidParameters = new[] { "Preview", "Trace" };

            try
            {
                // Make sure no invalid parameters are present
                if (objParseCommandLine.InvalidParametersPresent(strValidParameters))
                {
                    return;
                }


                // Query objParseCommandLine to see if various parameters are present
                if (objParseCommandLine.IsParameterPresent("Preview"))
                {
                    mPreviewMode = true;
                }

                if (objParseCommandLine.IsParameterPresent("Trace"))
                {
                    mTraceMode = true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(@"Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
            }
        }


        private static void ShowProgramHelp()
        {
            try
            {
                Console.WriteLine(
                    @"This program manages free space on Proto-x servers");
                Console.WriteLine();
                Console.WriteLine(@"Program syntax:" + Environment.NewLine +
                                  System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location) +
                                  @" [/Preview] [/Trace]");
                Console.WriteLine();

                Console.WriteLine(@"Use /Preview to preview the files that would be purged to free up space");
                Console.WriteLine();
                Console.WriteLine(@"Use /Trace to enable trace mode");
                Console.WriteLine();

                Console.WriteLine(@"Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine();

                Console.WriteLine(@"This is version " + System.Windows.Forms.Application.ProductVersion + @" (" +
                                  PROGRAM_DATE + @")");
                Console.WriteLine();

                Console.WriteLine(@"E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine(@"Website: http://panomics.pnnl.gov/ or http://www.sysbio.org/resources/staff/");
                Console.WriteLine();


                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                Console.WriteLine(@"Error displaying the program syntax: " + ex.Message);
            }
        }


    }
}
