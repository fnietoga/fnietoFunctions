using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Net;
using Microsoft.Data.Sqlite;
using System.Diagnostics;

namespace SQLite
{
    public static class SQLite
    {
        [FunctionName("SQLite")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("SQLite function was triggered!");

            // download SQLite file from URL
            var svgURL = "https://fnietosharepoint.blob.core.windows.net/sqlite/ASSAYS_BBDD.kdb";
            var fileName = "ASSAYS_BBDD.kdb";
            var fullFilePath = Path.GetTempPath() + "\\" + fileName;
            try
            {
                log.LogInformation($"Downloading file to [{fullFilePath}].");
                using (var client = new WebClient())
                {
                    Stopwatch stopWatch = new Stopwatch();
                    stopWatch.Start();
                    client.DownloadFile(svgURL, fullFilePath);
                    stopWatch.Stop();
                    log.LogInformation($"Download Completed in {stopWatch.Elapsed.TotalSeconds} seconds.");
                }
            }
            catch (Exception e)
            {
                log.LogInformation("Download Fail");
                log.LogInformation(e.Message);
            }

            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            //Use DB in project directory.  If it does not exist, create it:
            connectionStringBuilder.DataSource = fullFilePath;

            using (var connection = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                connection.Open();

                //Create a table (drop if already exists first):
                log.LogInformation("Dropping table favorite_beers");
                var delTableCmd = connection.CreateCommand();
                delTableCmd.CommandText = "DROP TABLE IF EXISTS favorite_beers";
                delTableCmd.ExecuteNonQuery();

                log.LogInformation("Creating table favorite_beers");
                var createTableCmd = connection.CreateCommand();
                createTableCmd.CommandText = "CREATE TABLE favorite_beers(name VARCHAR(50))";
                createTableCmd.ExecuteNonQuery();

                //Seed some data:
                using (var transaction = connection.BeginTransaction())
                {
                    var insertCmd = connection.CreateCommand();

                    log.LogInformation("Inserting 3 rows into table favorite_beers");
                    insertCmd.CommandText = "INSERT INTO favorite_beers VALUES('LAGUNITAS IPA')";
                    insertCmd.ExecuteNonQuery();

                    insertCmd.CommandText = "INSERT INTO favorite_beers VALUES('JAI ALAI IPA')";
                    insertCmd.ExecuteNonQuery();

                    insertCmd.CommandText = "INSERT INTO favorite_beers VALUES('RANGER IPA')";
                    insertCmd.ExecuteNonQuery();

                    transaction.Commit();
                }

                //Read the newly inserted data:
                log.LogInformation("Reading rows from table favorite_beers");
                var selectCmd = connection.CreateCommand();
                selectCmd.CommandText = "SELECT name FROM favorite_beers";

                using (var reader = selectCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var message = reader.GetString(0);
                        log.LogInformation($"Row readed: {message}");
                    }
                }
            }

            return new OkObjectResult($"SQLite function processed correctly");
        }
    }
}
