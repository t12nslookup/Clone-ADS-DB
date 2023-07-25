using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Clone_ADS_DB
{
    internal class Program
    {
        private static readonly bool debugMode = false;
        private static void Main(string[] args)
        {
            // Read configurations from the JSON file
            string jsonString = File.ReadAllText("config.json");
            Configuration config = JsonSerializer.Deserialize<Configuration>(jsonString);

            using OdbcConnection sourceConnection = new(config.SourceConnectionString);
            using NpgsqlConnection destinationConnection = new(config.DestinationConnectionString);
            sourceConnection.Open();
            destinationConnection.Open();

            // Copy tables from the list of table names
            foreach (string tableName in config.TableNames)
            {
                CopyTable(tableName, sourceConnection, destinationConnection);
            }

            sourceConnection.Close();
            destinationConnection.Close();
        }


        // Helper class to hold the configuration properties
        public class Configuration
        {
            public string SourceConnectionString { get; set; }
            public string DestinationConnectionString { get; set; }
            public string[] TableNames { get; set; }
        }
        public class IndexInfo
        {
            public string Name { get; set; }
            public string Key { get; set; }
        }

        private static string GenerateCreateTableQuery(string tableName, DataTable schemaTable)
        {
            string createTableQuery = $"CREATE TABLE IF NOT EXISTS {tableName} (";
            foreach (DataColumn column in schemaTable.Columns)
            {
                createTableQuery += $"\"{column.ColumnName.ToLower()}\" {GetPostgreSQLType(column.DataType)}, ";
            }
            createTableQuery = createTableQuery.TrimEnd(',', ' ') + ")";
            return createTableQuery;
        }


        private static string GetPostgreSQLType(Type type)
        {
            return Type.GetTypeCode(type) switch
            {
                TypeCode.Int32 => "integer",
                TypeCode.String => "text",
                TypeCode.DateTime => "timestamp",
                TypeCode.Boolean => "boolean",
                TypeCode.Decimal => "decimal",
                // Add more mappings for other data types as needed
                _ => throw new NotSupportedException($"Data type {type.Name} not supported."),
            };
        }

        private static void CopyTable(string tableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
        {
            string sourceTableName = tableName.ToUpper();
            string destinationTableName = "Copy_" + tableName.ToLower();

            // Retrieve the schema and column information from the source table
            try
            {
                Stopwatch tableWatch = new();
                Stopwatch totalWatch = new();
                string elapsedTime = null;
                totalWatch.Start();

                tableWatch.Start();
                // Gather the Schema Information
                OdbcCommand schemaCommand = new($"SELECT TOP 1 * FROM {sourceTableName}", sourceConnection);
                OdbcDataAdapter schemaAdapter = new(schemaCommand);
                DataTable schemaTable = new();
                _ = schemaAdapter.FillSchema(schemaTable, SchemaType.Source);

                CreateTableFromSource(schemaTable, destinationTableName, sourceConnection, destinationConnection);
                if (debugMode)
                {
                    tableWatch.Stop();
                    elapsedTime = PrettyFormatTime(tableWatch.ElapsedMilliseconds);
                    Console.WriteLine($"\r{destinationTableName}: table created - Time taken to create: {elapsedTime}".PadRight(Console.WindowWidth - 1));
                }

                // Truncate the destination table - clears out the old data.  needs removing when "upserts" work
                EmptyDestinationTable(destinationTableName, destinationConnection);

                // Now copy the data
                CopyDataFromSource(schemaTable, destinationTableName, sourceConnection, destinationConnection);

                totalWatch.Stop();
                elapsedTime = PrettyFormatTime(totalWatch.ElapsedMilliseconds);
                Console.WriteLine($"\r{destinationTableName}: Total Time taken: {elapsedTime}".PadRight(Console.WindowWidth - 1));
            }
            catch (OdbcException ex)
            {
                // Handle the exception if the table is encrypted or any other ODBC-related errors
                Console.WriteLine($"\rSkipping table '{tableName}' due to an error: {ex.Message}".PadRight(Console.WindowWidth - 1));
            }
        }

        private static string BuildUpsertCommand(string tableName, DataTable schemaTable)
        {
            // string primaryKeyColumn = GetPrimaryKeyColumns(schemaTable.TableName);
            string[] columnNames = schemaTable.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName.ToLower()}\"").ToArray();
            string insertColumns = string.Join(", ", columnNames);
            string updateColumns = string.Join(", ", columnNames.Select(c => $"{c} = EXCLUDED.{c}"));

            string[] parameterPlaceholders = schemaTable.Columns.Cast<DataColumn>().Select(c => $":{c.ColumnName}").ToArray();
            string insertValues = string.Join(", ", parameterPlaceholders);

            return $"INSERT INTO {tableName} ({insertColumns}) VALUES ({insertValues}) ";
            // UpSerts need constraints/indexes in place
            // + $"ON CONFLICT ({primaryKeyColumn}) DO UPDATE SET {updateColumns}";
        }

        private static string GetPrimaryKeyColumns(string tableName)
        {
            // Assuming you have a single primary key column named "id", you can adjust this based on your table's primary key.
            return tableName.ToLower() switch
            {
                "aisrpts" => "REPORT",
                // Add more mappings for other data types as needed
                _ => "id",
            };

        }

        private static int GetSourceRowCount(string tableName, OdbcConnection sourceConnection)
        {
            int rowCount = 0;

            using (OdbcCommand countCommand = new($"SELECT COUNT(*) FROM {tableName}", sourceConnection))
            {
                rowCount = Convert.ToInt32(countCommand.ExecuteScalar());
            }

            return rowCount;
        }


        private static void CreateTableFromSource(DataTable schemaTable, string destinationTableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
        {
            // Create a new table in the destination database based on the source schema
            NpgsqlCommand createTableCommand = new(GenerateCreateTableQuery(destinationTableName, schemaTable), destinationConnection);
            _ = createTableCommand.ExecuteNonQuery();

            // Retrieve index information from the source database
            List<IndexInfo> sourceIndexes = GetIndexesFromSource(schemaTable.TableName, sourceConnection);

            // Generate SQL statements for index creation on the destination database
            List<string> createIndexStatements = GenerateCreateIndexStatements(destinationTableName, sourceIndexes);

            // Execute the generated SQL statements on the destination database
            foreach (string createIndexStatement in createIndexStatements)
            {
                try
                {
                    using NpgsqlCommand createIndexCommand = new(createIndexStatement, destinationConnection);
                    _ = createIndexCommand.ExecuteNonQuery();
                    // Console.WriteLine($"\rIndex created successfully: {createIndexStatement}".PadRight(Console.WindowWidth - 1));
                }
                catch (Exception)
                {
                    // Ignore the exception if index creation fails
                }
            }
        }

        private static void EmptyDestinationTable(string destinationTableName, NpgsqlConnection destinationConnection)
        {
            NpgsqlCommand truncateCommand = new($"TRUNCATE TABLE {destinationTableName}", destinationConnection);
            _ = truncateCommand.ExecuteNonQuery();
        }

        private static void CopyDataFromSource(DataTable schemaTable, string destinationTableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
        {
            Stopwatch rowWatch = new();

            // Get the total count of rows from the source table
            long totalRowCount = GetSourceRowCount(schemaTable.TableName, sourceConnection);
            if (debugMode)
            {
                Console.WriteLine($"\r{destinationTableName}: Total rows in source table: {totalRowCount}".PadRight(Console.WindowWidth - 1));
            }

            // only bother to copy data, if there is data
            if (totalRowCount > 0)
            {
                // Set the page size for data fetching
                int pageSize = 1000; // You can adjust this value based on your needs

                // Calculate the number of pages based on the total count and page size
                int pageCount = (int)Math.Ceiling((double)totalRowCount / pageSize);

                rowWatch.Start();
                string elapsedTime;
                // Retrieve the data from the source table in smaller batches (pages)
                for (int currentPage = 1; currentPage <= pageCount; currentPage++)
                {
                    // Calculate the start row index for the current page
                    int startRow = ((currentPage - 1) * pageSize) + 1;

                    // Retrieve the data for the current page using the "START AT" clause
                    OdbcCommand selectCommand = new($"SELECT TOP {pageSize} START AT {startRow} * FROM {schemaTable.TableName}", sourceConnection);
                    OdbcDataAdapter dataAdapter = new(selectCommand);
                    DataTable dataTable = new();
                    _ = dataAdapter.Fill(dataTable);

                    // Copy the data to the destination table and measure the time taken
                    using (NpgsqlTransaction transaction = destinationConnection.BeginTransaction())
                    {
                        using (NpgsqlCommand upsertCommand = new(BuildUpsertCommand(destinationTableName, schemaTable), destinationConnection))
                        {
                            foreach (DataColumn column in schemaTable.Columns)
                            {
                                _ = upsertCommand.Parameters.AddWithValue($"@{column.ColumnName.ToLower()}", DBNull.Value);
                            }

                            foreach (DataRow row in dataTable.Rows)
                            {
                                foreach (DataColumn column in schemaTable.Columns)
                                {
                                    object value = row[column.ColumnName];
                                    upsertCommand.Parameters[$"@{column.ColumnName}"].Value = value; // != DBNull.Value ? value : null;
                                }

                                _ = upsertCommand.ExecuteNonQuery();
                            }
                        }

                        transaction.Commit();
                    }

                    // only bother reporting the "remaining time" if we're not finished.
                    if ((startRow + dataTable.Rows.Count - 1) < totalRowCount)
                    {
                        // Calculate the average time taken per row
                        long averageTimePerRow = rowWatch.ElapsedMilliseconds / (startRow + dataTable.Rows.Count - 1);

                        // Calculate the remaining rows to be copied
                        long remainingRows = totalRowCount - (startRow + dataTable.Rows.Count - 1);

                        // Calculate the estimated time remaining
                        TimeSpan estimatedTimeRemaining = TimeSpan.FromMilliseconds(averageTimePerRow * remainingRows);

                        // Display time taken for this page and the estimated time remaining
                        elapsedTime = PrettyFormatTime(rowWatch.ElapsedMilliseconds);
                        string remainingTime = PrettyFormatTime((long)estimatedTimeRemaining.TotalMilliseconds);
                        Console.Write($"\r{destinationTableName}: Rows copied: {startRow + dataTable.Rows.Count - 1} of {totalRowCount} - Time taken: {elapsedTime} - Estimated time remaining: {remainingTime}".PadRight(Console.WindowWidth - 1));
                        // Move to the next line after displaying the progress for this page
                    }
                }
                rowWatch.Stop();
                elapsedTime = PrettyFormatTime(rowWatch.ElapsedMilliseconds);
                if (debugMode)
                {
                    Console.WriteLine($"\r{destinationTableName}: Total Copy Time taken: {elapsedTime}".PadRight(Console.WindowWidth - 1));
                }
            }
        }
        private static List<IndexInfo> GetIndexesFromSource(string tableName, OdbcConnection sourceConnection)
        {
            List<IndexInfo> indexes = new();

            string query = $"SELECT trim(INDEX_NAME) as index_name, trim([KEY]) as [Key] FROM INDEXES WHERE TABLE_NAME = '{tableName}'";
            using (OdbcCommand command = new(query, sourceConnection))
            using (OdbcDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    string indexName = reader["INDEX_NAME"].ToString();
                    string key = reader["KEY"].ToString();

                    // Create an IndexInfo object and add it to the list
                    indexes.Add(new IndexInfo
                    {
                        Name = indexName,
                        Key = key
                    });
                }
            }

            return indexes;
        }

        private static List<string> GenerateCreateIndexStatements(string destinationTableName, List<IndexInfo> sourceIndexes)
        {
            List<string> createIndexStatements = new();

            foreach (IndexInfo index in sourceIndexes)
            {
                // ... Analyze the index information and generate SQL statements for index creation ...
                // Perform replacements in the "Key" value
                string key = ReplaceKey(index.Key);

                // Generate the SQL statement for index creation
                string createIndexStatement = $"CREATE INDEX {index.Name} ON {destinationTableName} ({key})";
                createIndexStatements.Add(createIndexStatement);
            }

            return createIndexStatements;
        }

        private static string ReplaceKey(string key)
        {
            // Handle the specific replacements for "+", "str", and "substr"
            key = Regex.Replace(key, @"\+", ",");
            key = Regex.Replace(key, @"upper\((.*?),(.*?)\)", "upper($1),upper($2)");
            key = Regex.Replace(key, @"str\(([^,]*?)\)", "$1");
            key = Regex.Replace(key, @"str\((.*?),(.*?),(.*?)\)", "substr($1, $2, $3)");

            return key;
        }

        private static string PrettyFormatTime(long milliseconds)
        {
            TimeSpan timeSpan = TimeSpan.FromMilliseconds(milliseconds);

            return timeSpan.TotalHours >= 1
                ? $"{(int)timeSpan.TotalHours} hours and {timeSpan.Minutes} minutes"
                : timeSpan.TotalMinutes >= 1
                    ? $"{(int)timeSpan.TotalMinutes} minutes and {timeSpan.Seconds} seconds"
                    : $"{timeSpan.Seconds} seconds";

        }
    }
}
