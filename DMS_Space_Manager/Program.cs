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
        private const string PROGRAM_DATE = "June 7, 2021";

        private static clsMainProgram mMainProgram;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        private static void Main(string[] args)
        {
            bool restart;

            var exeName = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name ?? string.Empty;
            var cmdLineParser = new CommandLineParser<CommandLineOptions>(exeName,
                PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE))
            {
                ProgramInfo = "This program manages free space on Proto-x servers",
                ContactInfo =
                    "Program written by Dave Clark and Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                    Environment.NewLine +
                    "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                    "Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov"
            };

            var parsed = cmdLineParser.ParseArgs(args, false);
            var options = parsed.ParsedResults;
            if (args.Length > 0 && !parsed.Success)
            {
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
                        mMainProgram = new clsMainProgram(options.PreviewMode, options.TraceMode);

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
                    ConsoleMsgUtils.ShowWarning("Exiting clsMainProcess.Main with error code = 1");
                    PRISM.Logging.FileLogger.FlushPendingMessages();
                    return;
                }
                Properties.Settings.Default.Reload();
            } while (restart);

            PRISM.Logging.FileLogger.FlushPendingMessages();
        }
    }
}
