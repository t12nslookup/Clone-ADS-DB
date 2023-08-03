using CommandLine;
using Npgsql;
using System;
using System.Data.Odbc;

namespace Clone_ADS_DB
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                ShowHelp();
                return;
            }
            Configuration config = AppConfiguration.Config;
            string command = "";

            // Parse the command-line arguments and store them in the options object
            _ = Parser.Default.ParseArguments<Options>(args)
                .WithParsed(options =>
                {
                    // Access the parsed options
                    command = options.Command.ToLower();

                    // Set the DebugMode and EchoMode in AppConfiguration
                    AppConfiguration.SetDebugMode(options.DebugMode);
                    AppConfiguration.SetEchoMode(options.EchoMode);
                })
                .WithNotParsed(errors =>
                {
                    // Handle any parsing errors if needed
                    Console.WriteLine("Invalid command-line arguments.");
                    ShowHelp();
                });

            using OdbcConnection sourceConnection = new(config.SourceConnectionString);
            using NpgsqlConnection destinationConnection = new(config.DestinationConnectionString);

            switch (command)
            {
                case "initial":

                    sourceConnection.Open();
                    destinationConnection.Open();

                    // Copy tables from the list of table names
                    foreach (string tableName in config.TableNames)
                    {
                        DatabaseHelper.CopyTable(tableName, sourceConnection, destinationConnection);
                    }

                    sourceConnection.Close();
                    destinationConnection.Close();

                    break;

                // Add more cases for other commands if needed
                case "increment" or "incremental":
                    // Handle command1
                    break;

                case "config":
                    // Handle command2
                    break;

                default:
                    Console.WriteLine($"Invalid command: '{command}'");
                    ShowHelp();
                    break;
            }
        }

        private static void ShowHelp()
        {
            // Display help text showing the available commands and usage instructions
            Console.WriteLine("Usage: YourExecutable.exe <command> [arguments]");
            Console.WriteLine("Available commands:");
            Console.WriteLine("   initial       : Create tables and import all data from the source to the destination, based on the configuration file.");
            Console.WriteLine("   increment(al) : Copy any updated data from the source, where a table is applicable for incremental, based on the configuration file.");
            Console.WriteLine("   config        : Offer a suggestion of table configuration based on the source database from the configuration.");
            // Add more commands with descriptions as needed
        }

        // At some time in the future it might be good to suggest columns to be used as the primary key for new tables
        private static string GetSourcePrimaryKeyColumns(string tableName)
        {
            // Assuming you have a single primary key column named "id", you can adjust this based on your table's primary key.
            return tableName.ToLower() switch
            {
                "aisrpts" => "REPORT",
                // Add more mappings for other data types as needed
                _ => "id",
            };
        }
    }
}
