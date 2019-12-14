using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace GitHubDataCollector
{
    public class ActionScheduler
    {
        private static readonly string settingFilePath = $"{Path.GetDirectoryName(Assembly.GetEntryAssembly().Location)}/scheduler_settings";

        public ActionSet ActionSet { get; private set; }

        public int TargetGitHubApiCoreRemain { get; private set; }
        public int TargetGitHubApiSearchRemain { get; private set; }

        public int TargetUserNumber { get; private set; }
        public int TargetOrganizationNumber { get; private set; }
        public int TargetRepositoryNumber { get; private set; }

        public int CurrentUserNumber { get; private set; }
        public int CurrentOrganizationNumber { get; private set; }
        public int CurrentRepositoryNumber { get; private set; }

        public ActionScheduler()
        {
            CreateSettingFile(settingFilePath);
            ReadSetting(settingFilePath);

            ActionSet = new ActionSet();
        }

        public void UpdateCurrentElementNumber()
        {
            CurrentUserNumber = DatabaseManager.Instance.User_Count();
            CurrentOrganizationNumber = DatabaseManager.Instance.Organization_Count();
            CurrentRepositoryNumber = DatabaseManager.Instance.Repository_Count();
        }

        public void CompleteCycle()
        {
            Console.WriteLine();
            Console.WriteLine($"{nameof(ActionScheduler)} complete cycle start, {DateTime.Now}");
            GitHubAPI.Instance.PrintCurrentRateLimit();
            Console.WriteLine();

            GetFollowers();
            UpdateAccountRepository();
            UpdateInvalidAccounts();

            Console.WriteLine();
            Console.WriteLine($"End of complete cycle, {DateTime.Now}");
            GitHubAPI.Instance.PrintCurrentRateLimit();
            Console.WriteLine();
        }

        public void RepoUpdateCycle()
        {
            Console.WriteLine();
            Console.WriteLine($"{nameof(ActionScheduler)} repo update cycle start, {DateTime.Now}");
            GitHubAPI.Instance.PrintCurrentRateLimit();
            Console.WriteLine();

            UpdateAccountRepository();

            Console.WriteLine();
            Console.WriteLine($"End of repo update cycle, {DateTime.Now}");
            GitHubAPI.Instance.PrintCurrentRateLimit();
            Console.WriteLine();
        }

        private void GetFollowers()
        {
            int coreMargin = Math.Max(GitHubAPI.Instance.CoreRateLimit.Remaining - TargetGitHubApiCoreRemain, 0);
            if (coreMargin == 0)
            {
                Console.WriteLine($"Insufficient core request for get followers, remain: {GitHubAPI.Instance.CoreRateLimit.Remaining}");
                return;
            }

            UpdateCurrentElementNumber();
            if (CurrentUserNumber > TargetUserNumber)
            {
                Console.WriteLine($"Reach {nameof(TargetUserNumber)}, current: {CurrentUserNumber}");
                return;
            }

            int followerLimit = 100;
            int userLimit = Math.Min(coreMargin / followerLimit, TargetUserNumber - CurrentUserNumber);
            ActionSet.User_GetAllFollower(userLimit, followerLimit);
        }

        private void UpdateAccountRepository()
        {
            int coreMargin = Math.Max(GitHubAPI.Instance.CoreRateLimit.Remaining - TargetGitHubApiCoreRemain, 0);
            if (coreMargin == 0)
            {
                Console.WriteLine($"Insufficient core request for update organizations repository, remain: {GitHubAPI.Instance.CoreRateLimit.Remaining}");
                return;
            }
            int pageLimit = 100;
            int limit = coreMargin / pageLimit;
            ActionSet.Repository_UpdateOrgsRepo(limit, pageLimit);

            coreMargin = Math.Max(GitHubAPI.Instance.CoreRateLimit.Remaining - TargetGitHubApiCoreRemain, 0);
            if (coreMargin == 0)
            {
                Console.WriteLine($"Insufficient core request for update users repository, remain: {GitHubAPI.Instance.CoreRateLimit.Remaining}");
                return;
            }
            limit = coreMargin / pageLimit;
            ActionSet.Repository_UpdateUsersRepo(limit, pageLimit);
        }

        private void UpdateInvalidAccounts()
        {
            int coreMargin = Math.Max(GitHubAPI.Instance.CoreRateLimit.Remaining - TargetGitHubApiCoreRemain, 0);
            if (coreMargin == 0)
            {
                Console.WriteLine($"Insufficient core request for update invalid organizations, remain: {GitHubAPI.Instance.CoreRateLimit.Remaining}");
                return;
            }
            ActionSet.Organization_UpdateInvalid(coreMargin);

            coreMargin = Math.Max(GitHubAPI.Instance.CoreRateLimit.Remaining - TargetGitHubApiCoreRemain, 0);
            if (coreMargin == 0)
            {
                Console.WriteLine($"Insufficient core request for update invalid users, remain: {GitHubAPI.Instance.CoreRateLimit.Remaining}");
                return;
            }
            ActionSet.User_UpdateInvalid(coreMargin, 100);
        }

        private void CreateSettingFile(string filePath)
        {
            try
            {
                if (File.Exists(filePath) == false)
                {
                    var defaultContents = new string[]
                    {
                        "500000",   // target user number
                        "10000",    // targer org number
                        "1000000",  // target repo number
                        "2500",     //target github core remain
                        "20",       // target github search remain
                    };
                    File.WriteAllLines(filePath, defaultContents);
                }
            }
            catch (Exception)
            {
                Console.WriteLine($"Failed to create setting file, path: {filePath}");
                throw;
            }
        }

        private void ReadSetting(string filePath)
        {
            try
            {
                string[] settings = File.ReadAllLines(filePath);

                TargetUserNumber = int.Parse(settings[0]);
                TargetOrganizationNumber = int.Parse(settings[1]);
                TargetRepositoryNumber = int.Parse(settings[2]);
                TargetGitHubApiCoreRemain = int.Parse(settings[3]);
                TargetGitHubApiSearchRemain = int.Parse(settings[4]);
            }
            catch (Exception)
            {
                Console.WriteLine($"Failed to read settings, path: {filePath}");
                throw;
            }
        }
    }
}
