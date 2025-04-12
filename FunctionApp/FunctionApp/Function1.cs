using System;
using Azure.Storage.Queues.Models;
using FunctionApp;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FunctionApp1
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(Function1))]
        async public Task Run([QueueTrigger("tickethub", Connection = "AzureWebJobsStorage")] QueueMessage response)
        {
            _logger.LogInformation($"C# Queue trigger function processed: {response.MessageText}");

            var order = System.Text.Json.JsonSerializer.Deserialize<Order>(response.MessageText);

            // get connection string from app settings
            string? connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("SQL connection string is not set in the environment variables.");
            }

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync(); // Note the ASYNC

                // Check if the ConcertOrders table exists, and if not, create it
                string checkTableQuery = @"
                    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ConcertOrders')
                    BEGIN
                        CREATE TABLE dbo.ConcertOrders
                        (
                            ConcertId INT,
                            Email NVARCHAR(255),
                            Name NVARCHAR(255),
                            Phone NVARCHAR(50),
                            Quantity INT,
                            CreditCard NVARCHAR(50),
                            Expiration NVARCHAR(50),
                            SecurityCode NVARCHAR(50),
                            Address NVARCHAR(255),
                            City NVARCHAR(100),
                            Province NVARCHAR(100),
                            PostalCode NVARCHAR(20),
                            Country NVARCHAR(100)
                        );
                    END";

                using (SqlCommand checkTableCmd = new SqlCommand(checkTableQuery, conn))
                {
                    await checkTableCmd.ExecuteNonQueryAsync();
                }

                // Insert the order data into the ConcertOrders table
                var insertQuery = @"
                    INSERT INTO dbo.ConcertOrders 
                    (
                        ConcertId, Email, Name, Phone, Quantity, CreditCard,
                        Expiration, SecurityCode, Address, City, Province,
                        PostalCode, Country
                    )
                    VALUES 
                    (
                        @concertId, @email, @name, @phone, @quantity, @creditCard,
                        @expiration, @securityCode, @address, @city, @province,
                        @postalCode, @country
                    )";

                using (SqlCommand insertCmd = new SqlCommand(insertQuery, conn))
                {
                    insertCmd.Parameters.AddWithValue("@concertId", order.ConcertId);
                    insertCmd.Parameters.AddWithValue("@email", order.Email);
                    insertCmd.Parameters.AddWithValue("@name", order.Name);
                    insertCmd.Parameters.AddWithValue("@phone", order.Phone);
                    insertCmd.Parameters.AddWithValue("@quantity", order.Quantity);
                    insertCmd.Parameters.AddWithValue("@creditCard", order.CreditCard);
                    insertCmd.Parameters.AddWithValue("@expiration", order.Expiration);
                    insertCmd.Parameters.AddWithValue("@securityCode", order.SecurityCode);
                    insertCmd.Parameters.AddWithValue("@address", order.Address);
                    insertCmd.Parameters.AddWithValue("@city", order.City);
                    insertCmd.Parameters.AddWithValue("@province", order.Province);
                    insertCmd.Parameters.AddWithValue("@postalCode", order.PostalCode);
                    insertCmd.Parameters.AddWithValue("@country", order.Country);

                    await insertCmd.ExecuteNonQueryAsync();
                }
            }
        }
    }
}
