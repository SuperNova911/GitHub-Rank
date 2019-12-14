using System;
using System.Collections.Generic;
using System.Text;

namespace GitHubDataCollector
{
    public class RepositoryManager
    {
        public IReadOnlyList<Repository> MostStarRepositories { get; private set; }

        #region Singleton
        private static RepositoryManager instance = null;
        private static readonly object instanceLock = new object();

        public static RepositoryManager Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (instance == null)
                    {
                        instance = new RepositoryManager();
                    }
                    return instance;
                }
            }
        }
        #endregion

        public void UpdateMostStarRepositories()
        {

        }
    }
}
