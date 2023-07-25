// Types.cs

namespace Clone_ADS_DB
{
    public class Configuration
    {
        public string SourceConnectionString { get; set; }
        public string DestinationConnectionString { get; set; }
        public string[] TableNames { get; set; }
        public string DebugMode { get; set; }
    }

    public class IndexInfo
    {
        public string Name { get; set; }
        public string Key { get; set; }
    }
}
