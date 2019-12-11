using MySql.Data.MySqlClient;
using Octokit;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace GitHub_Data_Collector
{
    public class Program
    {
        private static string serverAddress;
        private static string databaseName;
        private static string userId;
        private static string password;
        private static string token;

        static void Main(string[] args)
        {
            ReadSecret();

            Console.WriteLine("Connect to DB");
            DatabaseManager.Instance.ConnectToDB(serverAddress, databaseName, userId, password);

            Console.WriteLine("Init GitHub API");
            GitHubAPI.Instance.InitializeGitHubClient(new Credentials(token));

            LicenseCollection.Instance.UpdateCollectionFromDB();
            ActionSet actionSet = new ActionSet();

            GitHubAPI.Instance.Test();
            //actionSet.Repository_UpdateOrgsRepo(600);

            Console.WriteLine("Close DB");
            DatabaseManager.Instance.CloseDB();
        }

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
