using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using System.Threading.Tasks;

namespace DeveloperAssessment
{
    class Program
    {
        private static readonly string apiUrl = "https://datalake.apidocs.boldorange.com/collections/AkinExample";
        private static readonly string username = "acampbell";  // username
        private static readonly string password = "rVZ3FkzqcGawjEM952PWL6";  // password

        static async Task Main(string[] args)
        {
            string csvFilePath = "Developer Assessment Data.csv"; // Path to your CSV file
            
            try
            {
                // 1. Read CSV File
                var records = ReadCsv(csvFilePath);
                
                // 2. Send each record to the Data Lake
                foreach (var record in records)
                {
                    var success = await InsertDataIntoDataLake(record);
                    if (success)
                    {
                        Console.WriteLine($"Successfully inserted record: {JsonSerializer.Serialize(record)}");
                    }
                    else
                    {
                        Console.WriteLine($"Failed to insert record: {JsonSerializer.Serialize(record)}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        // Function to read the CSV file using CsvHelper
        private static List<dynamic> ReadCsv(string filePath)
        {
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                return new List<dynamic>(csv.GetRecords<dynamic>());
            }
        }

        // Function to insert data into the Bold Orange Data Lake
        private static async Task<bool> InsertDataIntoDataLake(dynamic record)
        {
            using (var client = new HttpClient())
            {
                // Set the authentication headers
                var authToken = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(authToken));

                // Create JSON content
                var jsonContent = new StringContent(JsonSerializer.Serialize(record), Encoding.UTF8, "application/json");

                // Post the data to the API
                var response = await client.PostAsync(apiUrl, jsonContent);

                // Return true if the insert was successful (201 Created status code)
                return response.StatusCode == System.Net.HttpStatusCode.Created;
            }
        }
    }
}
