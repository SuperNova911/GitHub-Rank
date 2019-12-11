using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using GitHubRankWeb.Core;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace GitHubRankWeb
{
    public class Program
    {
        private static string serverAddress;
        private static string databaseName;
        private static string userId;
        private static string password;

        public static void Main(string[] args)
        {
            ReadSecret();
            DatabaseManager.Instance.ConnectToDB(serverAddress, databaseName, userId, password);
            DataCollection.Instance.InitializeCollection();

            CreateHostBuilder(args).Build().Run();
            DatabaseManager.Instance.CloseDB();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });

        private static void ReadSecret()
        {
            string[] secrets = File.ReadAllLines($"{Path.GetDirectoryName(Assembly.GetEntryAssembly().Location)}/secret.txt");
            serverAddress = secrets[0];
            databaseName = secrets[1];
            userId = secrets[2];
            password = secrets[3];
        }
    }
}
