// StringManipulation.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;

namespace Clone_ADS_DB
{
    public static class StringManipulation
    {
        public static string ConvertToDestinationFilename(string sourceFilename)
        {
            // Remove square brackets from the source filename
            string destinationFilename = sourceFilename.Replace("[", "").Replace("]", "");

            // Replace non-alphabetical characters (slashes and dots) with underscores
            destinationFilename = Regex.Replace(destinationFilename, "[^a-zA-Z0-9]", "_");

            // Convert the final string to lowercase
            destinationFilename = destinationFilename.ToLower();

            return destinationFilename;
        }

        public static string GenerateCreateTableQuery(string tableName, DataTable schemaTable)
        {
            string createTableQuery = $"CREATE TABLE IF NOT EXISTS {tableName} (";
            foreach (DataColumn column in schemaTable.Columns)
            {
                createTableQuery += $"\"{column.ColumnName.ToLower()}\" {GetPostgreSQLType(column.DataType)}, ";
            }
            createTableQuery = createTableQuery.TrimEnd(',', ' ') + ")";
            return createTableQuery;
        }

        public static string BuildUpsertCommand(string tableName, DataTable schemaTable)
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


        public static string GetPostgreSQLType(Type type)
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

        public static List<string> GenerateCreateIndexStatements(string destinationTableName, List<IndexInfo> sourceIndexes)
        {
            List<string> createIndexStatements = new();

            foreach (IndexInfo index in sourceIndexes)
            {
                // ... Analyze the index information and generate SQL statements for index creation ...
                // Perform replacements in the "Key" value
                string key = ReplaceKey(index.Key);

                // Generate the SQL statement for index creation
                string createIndexStatement = $"CREATE INDEX {index.Name}_idx ON {destinationTableName} ({key})";
                createIndexStatements.Add(createIndexStatement);
            }

            return createIndexStatements;
        }

        public static string ReplaceKey(string key)
        {
            // Handle the specific replacements for "+", "str", and "substr"
            key = Regex.Replace(key, @"\+", ",");
            key = Regex.Replace(key, @"upper\((.*?),(.*?)\)", "upper($1),upper($2)");
            key = Regex.Replace(key, @"str\(([^,]*?)\)", "$1");
            key = Regex.Replace(key, @"str\((.*?),(.*?),(.*?)\)", "substr($1, $2, $3)");

            return key;
        }

        public static string PrettyFormatTime(long milliseconds)
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
