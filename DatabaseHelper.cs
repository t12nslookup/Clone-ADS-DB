// DatabaseHelper.cs
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using static Clone_ADS_DB.StringManipulation;

namespace Clone_ADS_DB
{
    public static class DatabaseHelper
    {
        // Add the methods related to database operations, such as CreateTableFromSource, EmptyDestinationTable, CopyDataFromSource,
        // GetIndexesFromSource, GenerateCreateIndexStatements, and other relevant methods.
        // You can also include the Configuration and IndexInfo classes from the Types.cs file in this project.
        public static void ExecuteStatement(NpgsqlCommand command)
        {
            if (AppConfiguration.EchoMode)
            {
                Console.WriteLine($"\rExecuting SQL: {command.CommandText}");
            }
            else
            {
                _ = command.ExecuteNonQuery();
            }
        }

        public static void CreateTableFromSource(DataTable schemaTable, string destinationTableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
        {
            Stopwatch tableWatch = new();
            tableWatch.Start();

            // Create a new table in the destination database based on the source schema
            NpgsqlCommand createTableCommand = new(GenerateCreateTableQuery(destinationTableName, schemaTable), destinationConnection);
            ExecuteStatement(createTableCommand);

            // Retrieve index information from the source database
            List<IndexInfo> sourceIndexes = GetIndexesFromSource(schemaTable.TableName, sourceConnection);

            // Generate SQL statements for index creation on the destination database
            List<string> createIndexStatements = GenerateCreateIndexStatements(destinationTableName, sourceIndexes);

            // Execute the index creation SQL statements on the destination database
            foreach (string createIndexStatement in createIndexStatements)
            {
                try
                {
                    using NpgsqlCommand createIndexCommand = new(createIndexStatement, destinationConnection);
                    ExecuteStatement(createIndexCommand);
                    if (AppConfiguration.DebugMode)
                    {
                        Console.WriteLine($"\rIndex created successfully: {createIndexStatement}".PadRight(Console.WindowWidth - 1));
                    }
                }
                catch (Exception ex)
                {
                    if (AppConfiguration.DebugMode)
                    {
                        // Handle the exception if index creation fails
                        Console.WriteLine($"\rFailed to create index: {createIndexStatement}".PadRight(Console.WindowWidth - 1));
                        Console.WriteLine($"\rError: {ex.Message}".PadRight(Console.WindowWidth - 1));
                    }
                }
            }

            // Output text showing the time for table + index creation
            if (AppConfiguration.DebugMode)
            {
                tableWatch.Stop();
                string elapsedTime = PrettyFormatTime(tableWatch.ElapsedMilliseconds);
                Console.WriteLine($"\r{destinationTableName}: Table created - Time taken to create: {elapsedTime}".PadRight(Console.WindowWidth - 1));
            }
        }

        public static void EmptyDestinationTable(string destinationTableName, NpgsqlConnection destinationConnection)
        {
            NpgsqlCommand truncateCommand = new($"TRUNCATE TABLE {destinationTableName}", destinationConnection);
            ExecuteStatement(truncateCommand);
        }

        public static void CopyDataFromSource(DataTable schemaTable, string destinationTableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
        {
            bool hasPrimaryKey = CheckIfTableHasPrimaryKey(destinationTableName, destinationConnection);
            string primaryKeyColumns = hasPrimaryKey ? GetPrimaryKeyColumns(destinationTableName, destinationConnection) : null;

            Stopwatch rowWatch = new();

            // Get the total count of rows from the source table
            long totalRowCount = GetSourceRowCount(schemaTable.TableName, sourceConnection);
            if (AppConfiguration.DebugMode)
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
                        using (NpgsqlCommand upsertCommand = new(BuildUpsertCommand(destinationTableName, schemaTable, primaryKeyColumns), destinationConnection))
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
                                    if (value is string stringValue)
                                    {
                                        // trim the string value, and if it's empty replace it with a Null
                                        value = string.IsNullOrWhiteSpace(stringValue) ? DBNull.Value : stringValue.Trim();
                                    }
                                    upsertCommand.Parameters[$"@{column.ColumnName}"].Value = value;
                                }

                                ExecuteStatement(upsertCommand);
                            }
                        }

                        transaction.Commit();
                    }

                    // only bother reporting the "remaining time" if we're not finished.
                    if ((startRow + dataTable.Rows.Count - 1) < totalRowCount)
                    {
                        // Calculate the remaining rows to be copied
                        long remainingRows = totalRowCount - (startRow + dataTable.Rows.Count - 1);

                        // Using the average time taken per row (multiply by remainingRows first, as small times get divided to 0)
                        // estimage the remaing time
                        long remainingTimeaverage = rowWatch.ElapsedMilliseconds * remainingRows / (startRow + dataTable.Rows.Count - 1);

                        // Calculate the estimated time remaining
                        TimeSpan estimatedTimeRemaining = TimeSpan.FromMilliseconds(remainingTimeaverage);

                        // Display time taken for this page and the estimated time remaining
                        elapsedTime = PrettyFormatTime(rowWatch.ElapsedMilliseconds);
                        string remainingTime = PrettyFormatTime((long)estimatedTimeRemaining.TotalMilliseconds);
                        Console.Write($"\r{destinationTableName}: Rows copied: {startRow + dataTable.Rows.Count - 1} of {totalRowCount} - Time taken: {elapsedTime} - Estimated time remaining: {remainingTime}".PadRight(Console.WindowWidth - 1));
                    }
                }
                rowWatch.Stop();
                elapsedTime = PrettyFormatTime(rowWatch.ElapsedMilliseconds);
                if (AppConfiguration.DebugMode)
                {
                    Console.WriteLine($"\r{destinationTableName}: Total Copy Time taken: {elapsedTime}".PadRight(Console.WindowWidth - 1));
                }
            }
        }

        public static List<IndexInfo> GetIndexesFromSource(string tableName, OdbcConnection sourceConnection)
        {
            List<IndexInfo> indexes = new();

            string query = $@"
    SELECT trim(INDEX_NAME) as index_name, trim([KEY]) as [Key] 
    FROM INDEXES 
    WHERE TABLE_NAME = '{tableName}' 
        and trim([KEY]) <> '' 
        and trim(INDEX_NAME) <> ''";
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
        public static int GetSourceRowCount(string tableName, OdbcConnection sourceConnection)
        {
            int rowCount = 0;

            using (OdbcCommand countCommand = new($"SELECT COUNT(*) FROM {tableName}", sourceConnection))
            {
                rowCount = Convert.ToInt32(countCommand.ExecuteScalar());
            }

            return rowCount;
        }

        public static void CopyTable(string tableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
        {
            string sourceTableName = tableName.ToUpper();
            string destinationTableName = ConvertToDestinationFilename(sourceTableName);

            bool tableExists = CheckIfTableExists(destinationTableName, destinationConnection);

            try
            {
                Stopwatch totalWatch = new();
                totalWatch.Start();

                // Gather the Schema Information
                OdbcCommand schemaCommand = new($"SELECT TOP 1 * FROM {sourceTableName}", sourceConnection);
                OdbcDataAdapter schemaAdapter = new(schemaCommand);
                DataTable schemaTable = new();
                _ = schemaAdapter.FillSchema(schemaTable, SchemaType.Source);

                // If the table doesn't exist, create it based on the source schema
                if (!tableExists)
                {

                    CreateTableFromSource(schemaTable, destinationTableName, sourceConnection, destinationConnection);
                }

                // Truncate the destination table - clears out the old data. (This can be removed when "upserts" work)
                EmptyDestinationTable(destinationTableName, destinationConnection);

                // Now copy the data from the source table to the destination table
                try
                {
                    CopyDataFromSource(schemaTable, destinationTableName, sourceConnection, destinationConnection);
                }
                catch (OdbcException ex)
                {
                    // Handle the exception if there's an error while copying the data
                    Console.WriteLine($"\rFailed to copy data for table '{tableName}': {ex.Message}".PadRight(Console.WindowWidth - 1));
                }
            }
            catch (OdbcException ex)
            {
                // Handle the exception if the table is encrypted or any other ODBC-related errors
                Console.WriteLine($"\rSkipping table '{tableName}' due to an error: {ex.Message}".PadRight(Console.WindowWidth - 1));
            }
        }

        public static bool CheckIfTableExists(string tableName, NpgsqlConnection connection)
        {
            using NpgsqlCommand command = new($"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{tableName}')", connection);
            return (bool)command.ExecuteScalar();
        }

        public static bool CheckIfTableHasPrimaryKey(string tableName, NpgsqlConnection connection)
        {
            using NpgsqlCommand command = new($"SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_name = '{tableName}' AND constraint_type = 'PRIMARY KEY'", connection);
            int count = Convert.ToInt32(command.ExecuteScalar());
            return count > 0;
        }

        public static string GetPrimaryKeyColumns(string tableName, NpgsqlConnection connection)
        {
            using NpgsqlCommand command = new($"SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{tableName}'::regclass AND i.indisprimary;", connection);
            using NpgsqlDataReader reader = command.ExecuteReader();
            List<string> primaryKeyColumns = new();
            while (reader.Read())
            {
                primaryKeyColumns.Add(reader.GetString(0));
            }
            return string.Join(", ", primaryKeyColumns);
        }

    }
}
