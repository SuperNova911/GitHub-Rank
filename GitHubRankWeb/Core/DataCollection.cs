using GitHubRankWeb.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GitHubRankWeb.Core
{
    public class DataCollection
    {
        public List<RankItem> MostStarUsers { get; private set; }
        public List<RankItem> MostStarOrganizations { get; private set; }
        public List<RankItem> MostStarRepositories { get; private set; }

        public List<RankItem> MostForkUsers { get; private set; }
        public List<RankItem> MostForkOrganizations { get; private set; }
        public List<RankItem> MostForkRepositories { get; private set; }

        public List<RankItem> MostRepoUsers { get; private set; }
        public List<RankItem> MostRepoOrganizations { get; private set; }

        public List<RankItem> MostFollowerUsers { get; private set; }
        public List<RankItem> MostFollowingUsers { get; private set; }

        #region Singleton
        private static DataCollection instance = null;
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

        public void InitializeCollection()
        {
            Console.WriteLine("Load Most star users");
            MostStarUsers = DatabaseManager.Instance.MostStarUser_SelectAll(200);
            Console.WriteLine("Load Most star orgs");
            MostStarOrganizations = DatabaseManager.Instance.MostStarOrganization_SelectAll(200);
            Console.WriteLine("Load Most star repos");
            MostStarRepositories = DatabaseManager.Instance.MostStarRepository_SelectAll(200);

            MostForkUsers = DatabaseManager.Instance.MostForkUser_SelectAll(200);
            MostForkOrganizations = DatabaseManager.Instance.MostForkOrganization_SelectAll(200);
            MostForkRepositories = DatabaseManager.Instance.MostForkRepository_SelectAll(200);

            MostRepoUsers = DatabaseManager.Instance.MostRepoUser_SelectAll(200);
            MostRepoOrganizations = DatabaseManager.Instance.MostRepoOrganization_SelectAll(200);

            MostFollowerUsers = DatabaseManager.Instance.MostFollowerUser_SelectAll(200);
            MostFollowingUsers = DatabaseManager.Instance.MostFollowingUser_SelectAll(200);
        }
    }
}
