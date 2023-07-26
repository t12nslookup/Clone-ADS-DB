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

        public static void CreateTableFromSource(DataTable schemaTable, string destinationTableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
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
        }

        public static void EmptyDestinationTable(string destinationTableName, NpgsqlConnection destinationConnection)
        {
            NpgsqlCommand truncateCommand = new($"TRUNCATE TABLE {destinationTableName}", destinationConnection);
            _ = truncateCommand.ExecuteNonQuery();
        }

        public static void CopyDataFromSource(DataTable schemaTable, string destinationTableName, OdbcConnection sourceConnection, NpgsqlConnection destinationConnection)
        {
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
                if (AppConfiguration.DebugMode)
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


    }
}
