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
    internal static class Program
    {
        private const string PROGRAM_DATE = "January 7, 2023";

        private static MainProgram mMainProgram;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        private static void Main(string[] args)
        {
            bool restart;

            var exeName = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name ?? string.Empty;

            var parser = new CommandLineParser<CommandLineOptions>(exeName,
                PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE))
            {
                ProgramInfo = "This program manages free space on Proto-x servers",
                ContactInfo =
                    "Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                    Environment.NewLine +
                    "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                    "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics"
            };

            var result = parser.ParseArgs(args, false);
            var options = result.ParsedResults;

            if (args.Length > 0 && !result.Success)
            {
                if (parser.CreateParamFileProvided)
                {
                    return;
                }

                // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(1500);
                return;
            }

            do
            {
                try
                {
                    if (mMainProgram == null)
                    {
                        //Initialize the main execution class
                        mMainProgram = new MainProgram(options.PreviewMode, options.TraceMode);

                        if (!mMainProgram.InitMgr())
                        {
                            PRISM.Logging.FileLogger.FlushPendingMessages();
                            return;
                        }
                    }
                    restart = mMainProgram.PerformSpaceManagement();
                    mMainProgram = null;
                }
                catch (Exception ex)
                {
                    var errMsg = "Critical exception starting application: " + ex.Message;
                    ConsoleMsgUtils.ShowWarning(errMsg + "; " + StackTraceFormatter.GetExceptionStackTrace(ex));
                    ConsoleMsgUtils.ShowWarning("Exiting Program.Main with error code = 1");
                    PRISM.Logging.FileLogger.FlushPendingMessages();
                    return;
                }
                Properties.Settings.Default.Reload();
            } while (restart);

            PRISM.Logging.FileLogger.FlushPendingMessages();
        }
    }
}
