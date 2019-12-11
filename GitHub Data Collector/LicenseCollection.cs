using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace GitHub_Data_Collector
{
    public class LicenseCollection
    {
        private List<License> licenses = new List<License>();

        private LicenseCollection()
        {
        }

        public ReadOnlyDictionary<string, License> LicenseDictionary { get; private set; }

        #region Singleton
        private static LicenseCollection instance = null;
        private static readonly object instanceLock = new object();

        public static LicenseCollection Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (instance == null)
                    {
                        instance = new LicenseCollection();
                    }
                    return instance;
                }
            }
        }
        #endregion

        public void UpdateCollectionFromDB()
        {
            if (DatabaseManager.Instance.Connected == false)
            {
                Console.WriteLine("데이터베이스가 연결되지 않았습니다");
                return;
            }

            licenses = DatabaseManager.Instance.License_SelectAll(int.MaxValue);
            LicenseDictionary = new ReadOnlyDictionary<string, License>(licenses.ToDictionary(k => k.Key, v => v));
        }
    }
}
