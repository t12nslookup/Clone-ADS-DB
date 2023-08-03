using System.IO;
using System.Text.Json;

namespace Clone_ADS_DB
{
    public static class AppConfiguration
    {
        private static Configuration config;

        public static Configuration Config
        {
            get
            {
                if (config == null)
                {
                    LoadConfiguration();
                }

                return config;
            }
        }

        public static bool DebugMode { get; private set; }
        public static bool EchoMode { get; private set; }
        public static void SetDebugMode(bool value)
        {
            DebugMode = value;
        }

        public static void SetEchoMode(bool value)
        {
            EchoMode = value;
        }

        private static void LoadConfiguration()
        {
            string jsonString = File.ReadAllText("config.json");
            config = JsonSerializer.Deserialize<Configuration>(jsonString);
        }
    }
}
