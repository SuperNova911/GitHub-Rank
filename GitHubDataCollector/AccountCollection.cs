using System;
using System.Collections.Generic;
using System.Text;

namespace GitHubDataCollector
{
    public class AccountCollection
    {
        private AccountCollection()
        {

        }

        public HashSet<long> AccountIds { get; private set; }

        #region Singleton
        private static AccountCollection instance = null;
        private static readonly object instanceLock = new object();

        public static AccountCollection Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (instance == null)
                    {
                        instance = new AccountCollection();
                    }
                    return instance;
                }
            }
        }
        #endregion

        public void UpdateIdFromDB()
        {
            AccountIds = new HashSet<long>(DatabaseManager.Instance.AccountId_SelectAll(int.MaxValue));
        }
    }
}
