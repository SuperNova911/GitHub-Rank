using GitHubRankWeb.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GitHubRankWeb.Core
{
    public class DataCollection
    {
        private DataCollection()
        {
            LastFetchedTime = DateTime.Now;
            FetchPeriod = TimeSpan.FromHours(1);
        }

        public List<RankItem> MostStarUsers
        {
            get
            {
                CheckOutdated();
                return mostStarUsers;
            }
            private set
            {
                mostStarUsers = value;
            }
        }
        public List<RankItem> MostStarOrganizations
        {
            get
            {
                CheckOutdated();
                return mostStarOrganizations;
            }
            private set
            {
                mostStarOrganizations = value;
            }
        }
        public List<RankItem> MostStarRepositories
        {
            get
            {
                CheckOutdated();
                return mostStarRepositories;
            }
            private set
            {
                mostStarRepositories = value;
            }
        }

        public List<RankItem> MostForkUsers
        {
            get
            {
                CheckOutdated();
                return mostForkUsers;
            }
            private set
            {
                mostForkUsers = value;
            }
        }
        public List<RankItem> MostForkOrganizations
        {
            get
            {
                CheckOutdated();
                return mostForkOrganizations;
            }
            private set
            {
                mostForkOrganizations = value;
            }
        }
        public List<RankItem> MostForkRepositories
        {
            get
            {
                CheckOutdated();
                return mostForkRepositories;
            }
            private set
            {
                mostForkRepositories = value;
            }
        }

        public List<RankItem> MostRepoUsers
        {
            get
            {
                CheckOutdated();
                return mostRepoUsers;
            }
            private set
            {
                mostRepoUsers = value;
            }
        }
        public List<RankItem> MostRepoOrganizations
        {
            get
            {
                CheckOutdated();
                return mostRepoOrganizations;
            }
            private set
            {
                mostRepoOrganizations = value;
            }
        }

        public List<RankItem> MostFollowerUsers
        {
            get
            {
                CheckOutdated();
                return mostFollowerUsers;
            }
            private set
            {
                mostFollowerUsers = value;
            }
        }
        public List<RankItem> MostFollowingUsers
        {
            get
            {
                CheckOutdated();
                return mostFollowingUsers;
            }
            private set
            {
                mostFollowingUsers = value;
            }
        }

        public List<LanguageModel> MostLanguages
        {
            get
            {
                CheckOutdated();
                return mostLanguages;
            }
            private set
            {
                mostLanguages = value;
            }
        }
        public List<LicenseModel> MostLicenses
        {
            get
            {
                CheckOutdated();
                return mostLicenses;
            }
            private set
            {
                mostLicenses = value;
            }
        }

        public DateTime LastFetchedTime { get; private set; }
        public TimeSpan FetchPeriod { get; }

        #region Singleton
        private static DataCollection instance = null;
        private List<RankItem> mostStarUsers;
        private List<RankItem> mostStarOrganizations;
        private List<RankItem> mostStarRepositories;
        private List<RankItem> mostForkUsers;
        private List<RankItem> mostForkOrganizations;
        private List<RankItem> mostForkRepositories;
        private List<RankItem> mostRepoUsers;
        private List<RankItem> mostRepoOrganizations;
        private List<RankItem> mostFollowerUsers;
        private List<RankItem> mostFollowingUsers;
        private List<LanguageModel> mostLanguages;
        private List<LicenseModel> mostLicenses;
        private static readonly object instanceLock = new object();

        public static DataCollection Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (instance == null)
                    {
                        instance = new DataCollection();
                    }
                    return instance;
                }
            }
        }
        #endregion

        public void UpdateCollections()
        {
            MostStarUsers = DatabaseManagerWeb.Instance.MostStarUser_SelectAll(200);
            MostStarOrganizations = DatabaseManagerWeb.Instance.MostStarOrganization_SelectAll(200);
            MostStarRepositories = DatabaseManagerWeb.Instance.MostStarRepository_SelectAll(200);

            MostForkUsers = DatabaseManagerWeb.Instance.MostForkUser_SelectAll(200);
            MostForkOrganizations = DatabaseManagerWeb.Instance.MostForkOrganization_SelectAll(200);
            MostForkRepositories = DatabaseManagerWeb.Instance.MostForkRepository_SelectAll(200);

            MostRepoUsers = DatabaseManagerWeb.Instance.MostRepoUser_SelectAll(200);
            MostRepoOrganizations = DatabaseManagerWeb.Instance.MostRepoOrganization_SelectAll(200);

            MostFollowerUsers = DatabaseManagerWeb.Instance.MostFollowerUser_SelectAll(200);
            MostFollowingUsers = DatabaseManagerWeb.Instance.MostFollowingUser_SelectAll(200);

            MostLanguages = DatabaseManagerWeb.Instance.MostLanguage_SelectAll(100);
            MostLicenses = DatabaseManagerWeb.Instance.MostLicense_SelectAll(100);

            LastFetchedTime = DateTime.Now;
            Console.WriteLine($"Last fetched time: {LastFetchedTime}");
        }

        private void CheckOutdated()
        {
            if (DateTime.Now - LastFetchedTime > FetchPeriod)
            {
                Console.WriteLine("Outdated collections, fetch again");
                UpdateCollections();
            }
        }
    }
}
