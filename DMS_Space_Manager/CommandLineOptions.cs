using PRISM;

namespace Space_Manager
{
    public class CommandLineOptions
    {
        [Option("preview", HelpShowsDefault = false, HelpText = "Enables preview mode (run full check, but don't change files or the database). Also reports which files would be purged to free up space.")]
        public bool PreviewMode { get; set; }

        [Option("trace", HelpShowsDefault = false, HelpText = "Enables trace mode (extra output for debugging)")]
        public bool TraceMode { get; set; }
    }
}
