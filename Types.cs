// Types.cs

using CommandLine;

namespace Clone_ADS_DB
{
    public class Configuration
    {
        public string SourceConnectionString { get; set; }
        public string DestinationConnectionString { get; set; }
        public string[] TableNames { get; set; }
        public bool DebugMode { get; set; }
        public bool EchoMode { get; set; }
    }

    public class IndexInfo
    {
        public string Name { get; set; }
        public string Key { get; set; }
    }

    public class Options
    {
        [Option('d', "debug", Required = false, HelpText = "Enable debug mode.")]
        public bool DebugMode { get; set; } = false;

        [Option('e', "echo", Required = false, HelpText = "Enable echo mode.")]
        public bool EchoMode { get; set; } = false;

        [Value(0, Required = true, HelpText = "Command to execute (e.g., initial, increment, config).")]
        public string Command { get; set; }
    }
}
