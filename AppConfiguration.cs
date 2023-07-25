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

        public static bool DebugMode
        {
            get
            {
                if (config == null)
                {
                    LoadConfiguration();
                }

                return config.DebugMode == "true";
            }
        }

        private static void LoadConfiguration()
        {
            string jsonString = File.ReadAllText("config.json");
            config = JsonSerializer.Deserialize<Configuration>(jsonString);
        }
    }
}
