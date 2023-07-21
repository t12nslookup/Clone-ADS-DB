using Npgsql;
using System;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace Clone_ADS_DB
{
    internal class Program
    {
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
            string destinationTableName = tableName.ToLower();

            // Optional: Add a prefix to the destination table name
            string destinationTablePrefix = "Copy_";

            // Retrieve the schema and column information from the source table
            try
            {
                OdbcCommand schemaCommand = new($"SELECT TOP 1 * FROM {sourceTableName}", sourceConnection);
                OdbcDataAdapter schemaAdapter = new(schemaCommand);
                DataTable schemaTable = new();
                _ = schemaAdapter.FillSchema(schemaTable, SchemaType.Source);

                // Create a new table in the destination database with the prefixed name and based on the source schema
                Stopwatch tableWatch = new();
                Stopwatch rowWatch = new();
                tableWatch.Start();
                rowWatch.Start();
                NpgsqlCommand createTableCommand = new(GenerateCreateTableQuery(destinationTablePrefix + destinationTableName, schemaTable), destinationConnection);
                _ = createTableCommand.ExecuteNonQuery();
                rowWatch.Stop();
                Console.WriteLine($"{destinationTableName}: table created - Time taken to create: {rowWatch.ElapsedMilliseconds} milliseconds");

                // Truncate the destination table - clears out the old data.  needs removing when "upserts" work
                NpgsqlCommand truncateCommand = new($"TRUNCATE TABLE {destinationTablePrefix + destinationTableName}", destinationConnection);
                _ = truncateCommand.ExecuteNonQuery();

                // Get the total count of rows from the source table
                long totalRowCount = GetSourceRowCount(sourceTableName, sourceConnection);
                Console.WriteLine($"{destinationTableName}: Total rows in source table: {totalRowCount}");

                // Set the page size for data fetching
                int pageSize = 1000; // You can adjust this value based on your needs

                // Calculate the number of pages based on the total count and page size
                int pageCount = (int)Math.Ceiling((double)totalRowCount / pageSize);

                rowWatch.Restart();
                // Retrieve the data from the source table in smaller batches (pages)
                for (int currentPage = 1; currentPage <= pageCount; currentPage++)
                {
                    // Calculate the start row index for the current page
                    int startRow = ((currentPage - 1) * pageSize) + 1;

                    rowWatch.Restart();
                    // Retrieve the data for the current page using the "START AT" clause
                    OdbcCommand selectCommand = new($"SELECT TOP {pageSize} START AT {startRow} * FROM {sourceTableName}", sourceConnection);
                    OdbcDataAdapter dataAdapter = new(selectCommand);
                    DataTable dataTable = new();
                    _ = dataAdapter.Fill(dataTable);
                    rowWatch.Stop();
                    Console.WriteLine($"{destinationTableName}: rows read: {startRow}-{startRow + dataTable.Rows.Count - 1} - Time taken to read: {rowWatch.ElapsedMilliseconds} milliseconds");

                    // Copy the data to the destination table and measure the time taken
                    rowWatch.Restart();
                    using (NpgsqlTransaction transaction = destinationConnection.BeginTransaction())
                    {
                        using (NpgsqlCommand upsertCommand = new(BuildUpsertCommand(destinationTablePrefix + destinationTableName, schemaTable), destinationConnection))
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
                    rowWatch.Stop();
                    Console.WriteLine($"{destinationTableName}: rows written: {dataTable.Rows.Count} - Time taken to write: {rowWatch.ElapsedMilliseconds} milliseconds");
                }
                tableWatch.Stop();
                Console.WriteLine($"{destinationTableName}: Total Time taken: {tableWatch.ElapsedMilliseconds} milliseconds");
            }
            catch (OdbcException ex)
            {
                // Handle the exception if the table is encrypted or any other ODBC-related errors
                Console.WriteLine($"Skipping table '{tableName}' due to an error: {ex.Message}");
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

    }
}
