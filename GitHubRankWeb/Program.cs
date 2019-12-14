using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using GitHubDataCollector;
using GitHubRankWeb.Core;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Octokit;

namespace GitHubRankWeb
{
    public class Program
    {
        private static string serverAddress;
        private static string databaseName;
        private static string userId;
        private static string password;
        private static string token;

        public static void Main(string[] args)
        {
            ReadSecret();
            Console.WriteLine("Connect to DB");
            DatabaseManagerWeb.Instance.ConnectToDB(serverAddress, databaseName, userId, password);

            Console.WriteLine("Initialize GitHub Api");
            GitHubAPI.Instance.InitializeGitHubClient(new Credentials(token));

            Console.WriteLine("Initialize Data Collections");
            DataCollection.Instance.UpdateCollections();

            Console.WriteLine("Initialize Web Service");
            CreateHostBuilder(args).Build().Run();
            DatabaseManagerWeb.Instance.CloseDB();
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
            token = secrets[4];
        }
    }
}
