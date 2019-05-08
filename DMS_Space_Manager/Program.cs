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
    /// <summary>
    /// Application startup class
    /// </summary>
    static class Program
    {

        private const string PROGRAM_DATE = "May 7, 2019";

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

            var commandLineParser = new clsParseCommandLine();

            // Look for /T or /Test on the command line
            // If present, this means "code test mode" is enabled
            if (commandLineParser.ParseCommandLine())
            {
                SetOptionsUsingCommandLineParameters(commandLineParser);
            }

            if (commandLineParser.NeedToShowHelp)
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
                            PRISM.Logging.FileLogger.FlushPendingMessages();
                            return;
                        }
                    }
                    restart = m_MainProgram.PerformSpaceManagement();
                    m_MainProgram = null;
                }
                catch (Exception ex)
                {
                    var errMsg = "Critical exception starting application: " + ex.Message;
                    ConsoleMsgUtils.ShowWarning(errMsg + "; " + StackTraceFormatter.GetExceptionStackTrace(ex));
                    ConsoleMsgUtils.ShowWarning("Exiting clsMainProcess.Main with error code = 1");
                    PRISM.Logging.FileLogger.FlushPendingMessages();
                    return;
                }
                Properties.Settings.Default.Reload();
            } while (restart);

            PRISM.Logging.FileLogger.FlushPendingMessages();
        }


        private static void SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false

            var strValidParameters = new[] { "Preview", "Trace" };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(strValidParameters))
                {
                    return;
                }


                // Query objParseCommandLine to see if various parameters are present
                if (commandLineParser.IsParameterPresent("Preview"))
                {
                    mPreviewMode = true;
                }

                if (commandLineParser.IsParameterPresent("Trace"))
                {
                    mTraceMode = true;
                }
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error parsing the command line parameters", ex);
            }
        }

        private static void ShowProgramHelp()
        {
            try
            {
                Console.WriteLine("This program manages free space on Proto-x servers");
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine +
                                  System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location) +
                                  " [/Preview] [/Trace]");
                Console.WriteLine();

                Console.WriteLine("Use /Preview to preview the files that would be purged to free up space");
                Console.WriteLine();
                Console.WriteLine("Use /Trace to enable trace mode");
                Console.WriteLine();

                Console.WriteLine("Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine();

                Console.WriteLine("This is version " + PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE));
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov");
                Console.WriteLine();


                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error displaying the program syntax", ex);
            }
        }


    }
}
