using GitHub_Data_Collector;
using GitHubRankWeb.Models;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Security.Principal;
using System.Threading.Tasks;

namespace GitHubRankWeb.Core
{
    public class DatabaseManager
    {
        private MySqlConnection connection = null;
        private Queue<MySqlCommand> commands = new Queue<MySqlCommand>();

        public bool Connected => connection != null && connection.State == ConnectionState.Open;

        private DatabaseManager()
        {

        }

        #region Singleton
        private static DatabaseManager instance = null;
        private static readonly object instanceLock = new object();

        public static DatabaseManager Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (instance == null)
                    {
                        instance = new DatabaseManager();
                    }
                    return instance;
                }
            }
        }
        #endregion

        public bool ConnectToDB(string serverAddress, string databaseName, string userId, string password)
        {
            if (connection != null && connection.State == ConnectionState.Open)
            {
                Console.WriteLine("이미 데이터베이스에 연결 중입니다");
                return true;
            }

            try
            {
                string connectionString = $"Server={serverAddress};Database={databaseName};Uid={userId};Pwd={password};";
                connection = new MySqlConnection(connectionString);
                connection.Open();
                return true;
            }
            catch (MySqlException e)
            {
                Console.WriteLine(e);
                connection = null;
                return false;
            }
        }

        public void CloseDB()
        {
            if (connection == null || connection.State == ConnectionState.Closed)
            {
                Console.WriteLine("데이터베이스에 연결 중이 아닙니다");
            }
            connection.Close();
        }

        public List<RankItem> MostStarUser_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_star_user` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("total_star_count"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostStarOrganization_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_star_organization` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("total_star_count"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostStarRepository_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_star_repository_with_account` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string repoName = dataReader.GetString("name");
                    string userLogin = dataReader.GetString("owner_login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = $"{userLogin}/{repoName}",
                        Score = dataReader.GetInt32("stargazers_count"),
                        Reference = $"/account/{userLogin}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostForkUser_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_fork_user` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("total_fork_count"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostForkOrganization_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_fork_organization` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("total_fork_count"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostForkRepository_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT repo.*, account.login as owner_login, account.avatar_url " +
                "FROM `github_rank`.`most_fork_repository` as repo JOIN `github_rank`.`account` as account ON repo.owner_id = account.id " +
                "ORDER BY repo.forks_count DESC LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("owner_login");
                    string repoName = dataReader.GetString("name");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = $"{login}/{repoName}",
                        Score = dataReader.GetInt32("forks_count"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostRepoUser_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_public_repo_user` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("public_repos"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostRepoOrganization_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_public_repo_organization` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("public_repos"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostFollowerUser_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_follower_user` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("followers"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        public List<RankItem> MostFollowingUser_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_following_user` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                int rank = 1;
                var items = new List<RankItem>();
                while (dataReader.Read())
                {
                    string login = dataReader.GetString("login");
                    items.Add(new RankItem()
                    {
                        Rank = rank++,
                        AvatarUrl = dataReader.GetString("avatar_url"),
                        Name = login,
                        Score = dataReader.GetInt32("following"),
                        Reference = $"/account/{login}"
                    });
                }
                return items;
            }
        }

        #region Account
        public void Account_InsertOrUpdate(Account account)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            if (account.Valid)
            {
                command.CommandText = "INSERT INTO `github_rank`.`account` (`id`, `node_id`, `type_id`, `login`, `following`, `followers`, `public_repos`, `avatar_url`, `html_url`, `api_url`, `blog_url`, `email`, `bio`, `company`, `location`, `created_at`, `fetched_at`, `valid`) " +
                    "VALUES (@id, @node_id, @type_id, @login, @following, @followers, @public_repos, @avatar_url, @html_url, @api_url, @blog_url, @email, @bio, @company, @location, @created_at, @fetched_at, @valid)" +
                    "ON DUPLICATE KEY UPDATE `type_id` = @type_id, `login` = @login, `following` = @following, `followers` = @followers, `public_repos` = @public_repos, `avatar_url` = @avatar_url, `html_url` = @html_url, `api_url` = @api_url, `blog_url` = @blog_url, `email` = @email, `bio` = @bio, `company` = @company, `location` = @location, `created_at` = @created_at, `fetched_at` = @fetched_at, `valid` = @valid;";
            }
            else
            {
                command.CommandText = "INSERT INTO `github_rank`.`account` (`id`, `node_id`, `type_id`, `login`, `following`, `followers`, `public_repos`, `avatar_url`, `html_url`, `api_url`, `blog_url`, `email`, `bio`, `company`, `location`, `created_at`, `fetched_at`, `valid`) " +
                    "VALUES (@id, @node_id, @type_id, @login, @following, @followers, @public_repos, @avatar_url, @html_url, @api_url, @blog_url, @email, @bio, @company, @location, @created_at, @fetched_at, @valid)" +
                    "ON DUPLICATE KEY UPDATE `type_id` = @type_id, `login` = @login, `avatar_url` = @avatar_url, `html_url` = @html_url, `api_url` = @api_url;";
            }

            command.Parameters.AddWithValue("@id", account.Id);
            command.Parameters.AddWithValue("@node_id", account.NodeId);
            command.Parameters.AddWithValue("@type_id", account.TypeId);
            command.Parameters.AddWithValue("@login", account.Login);
            command.Parameters.AddWithValue("@following", account.Following);
            command.Parameters.AddWithValue("@followers", account.Followers);
            command.Parameters.AddWithValue("@public_repos", account.PublicRepos);
            command.Parameters.AddWithValue("@avatar_url", account.AvatarUrl);
            command.Parameters.AddWithValue("@html_url", account.HtmlUrl);
            command.Parameters.AddWithValue("@api_url", account.ApiUrl);
            command.Parameters.AddWithValue("@blog_url", account.BlogUrl);
            command.Parameters.AddWithValue("@email", account.Email);
            command.Parameters.AddWithValue("@bio", account.Bio);
            command.Parameters.AddWithValue("@company", account.Company);
            command.Parameters.AddWithValue("@location", account.Location);
            command.Parameters.AddWithValue("@created_at", account.CreatedAt);
            command.Parameters.AddWithValue("@fetched_at", account.FetchedAt);
            command.Parameters.AddWithValue("@valid", account.Valid);

            command.ExecuteNonQuery();

            switch (account)
            {
                case User user:
                    User_InsertOrUpdate(user);
                    break;
                case Organization organization:
                    Organization_InsertOrUpdate(organization);
                    break;
                default:
                    Console.WriteLine($"관리되지 않는 {nameof(Account)} 타입입니다, Name: {account.GetType().Name}");
                    break;
            }
        }

        public List<User> User_SelectInvalidAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`user_account_invalid` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var users = new List<User>();
                while (dataReader.Read())
                {
                    users.Add(ParseUser(dataReader));
                }
                return users;
            }
        }

        public List<string> UserLogin_SelectNeedRepoUpdate(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`need_repo_update_user` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var userLogins = new List<string>();
                while (dataReader.Read())
                {
                    userLogins.Add(dataReader.GetString("login"));
                }
                return userLogins;
            }
        }

        public List<Organization> Organization_SelectInvalidAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`organization_account_invalid` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var organizations = new List<Organization>();
                while (dataReader.Read())
                {
                    organizations.Add(ParseOrganization(dataReader));
                }
                return organizations;
            }
        }

        public List<string> OrganizationLogin_SelectNeedRepoUpdate(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`need_repo_update_organization` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var orgLogins = new List<string>();
                while (dataReader.Read())
                {
                    orgLogins.Add(dataReader.GetString("login"));
                }
                return orgLogins;
            }
        }

        private void User_InsertOrUpdate(User user)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            if (user.Valid)
            {
                command.CommandText = "INSERT INTO `github_rank`.`user` (`account_id`, `site_admin`, `suspended`, `suspended_at`, `updated_at`)" +
                    "VALUES (@account_id, @site_admin, @suspended, @suspended_at, @updated_at)" +
                    "ON DUPLICATE KEY UPDATE `site_admin` = @site_admin, `suspended` = @suspended, `suspended_at` = @suspended_at, `updated_at` = @updated_at;";
            }
            else
            {
                command.CommandText = "INSERT INTO `github_rank`.`user` (`account_id`, `site_admin`, `suspended`, `suspended_at`, `updated_at`)" +
                    "VALUES (@account_id, @site_admin, @suspended, @suspended_at, @updated_at)" +
                    "ON DUPLICATE KEY UPDATE `account_id` = @account_id";
            }

            command.Parameters.AddWithValue("@account_id", user.Id);
            command.Parameters.AddWithValue("@site_admin", user.SiteAdmin);
            command.Parameters.AddWithValue("@suspended", user.Suspended);
            command.Parameters.AddWithValue("@suspended_at", user.SuspendedAt);
            command.Parameters.AddWithValue("@updated_at", user.UpdatedAt);
            command.ExecuteNonQuery();
        }

        private void Organization_InsertOrUpdate(Organization organization)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "INSERT INTO `github_rank`.`organization` (`account_id`) " +
                "VALUES (@account_id)" +
                "ON DUPLICATE KEY UPDATE `account_id` = @account_id;";
            command.Parameters.AddWithValue("@account_id", organization.Id);
            command.ExecuteNonQuery();
        }
        #endregion

        #region License
        public void Lisence_InsertOrUpdate(GitHub_Data_Collector.License license)
        {
            var command = new MySqlCommand();
            command.CommandText = "INSERT INTO `github_rank`.`license` (`key`, `node_id`, `name`, `spdx_id`, `api_url`) " +
                "VALUES (@key, @node_id, @name, @spdx_id, @api_url)" +
                "ON DUPLICATE KEY UPDATE `name` = @name, `spdx_id` = @spdx_id, `api_url` = @api_url;";
            command.Connection = connection;

            command.Parameters.AddWithValue("@key", license.Key);
            command.Parameters.AddWithValue("@node_id", license.NodeId);
            command.Parameters.AddWithValue("@name", license.Name);
            command.Parameters.AddWithValue("@spdx_id", license.SpdxId);
            command.Parameters.AddWithValue("@api_url", license.ApiUrl);

            command.ExecuteNonQuery();
        }

        public List<GitHub_Data_Collector.License> License_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.CommandText = "SELECT * FROM `github_rank`.`license` LIMIT @limit";
            command.Connection = connection;
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var licenses = new List<GitHub_Data_Collector.License>();
                while (dataReader.Read())
                {
                    licenses.Add(ParseLicense(dataReader));
                }
                return licenses;
            }
        }
        #endregion

        #region Repository
        public void Repository_InsertOrUpdate(Repository repo)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            if (repo.Valid)
            {
                command.CommandText = "INSERT INTO `github_rank`.`repository` (`id`, `node_id`, `owner_id`, `name`, `description`, `language`, `license_key`, `subscribers_count`, `stargazers_count`, `forks_count`, `fork`, `archived`, `html_url`, `api_url`, `created_at`, `updated_at`, `parent_id`, `source_id`, `fetched_at`, `valid`)" +
                    "VALUES (@id, @node_id, @owner_id, @name, @description, @language, @license_key, @subscribers_count, @stargazers_count, @forks_count, @fork, @archived, @html_url, @api_url, @created_at, @updated_at, @parent_id, @source_id, @fetched_at, @valid)" +
                    "ON DUPLICATE KEY UPDATE `owner_id` = @owner_id, `name` = @name, `description` = @description, `language` = @language, `license_key` = @license_key, `subscribers_count` = @subscribers_count, `stargazers_count` = @stargazers_count, `forks_count` = @forks_count, `fork` = @fork, `archived` = @archived, `html_url` = @html_url, `api_url` = @api_url, `created_at` = @created_at, `updated_at` = @updated_at, `fetched_at` = @fetched_at, `valid` = @valid;";
            }
            else
            {
                command.CommandText = "INSERT INTO `github_rank`.`repository` (`id`, `node_id`, `owner_id`, `name`, `description`, `language`, `license_key`, `subscribers_count`, `stargazers_count`, `forks_count`, `fork`, `archived`, `html_url`, `api_url`, `created_at`, `updated_at`, `parent_id`, `source_id`, `fetched_at`, `valid`)" +
                    "VALUES (@id, @node_id, @owner_id, @name, @description, @language, @license_key, @subscribers_count, @stargazers_count, @forks_count, @fork, @archived, @html_url, @api_url, @created_at, @updated_at, @parent_id, @source_id, @fetched_at, @valid)" +
                    "ON DUPLICATE KEY UPDATE `owner_id` = @owner_id, `name` = @name, `description` = @description, `language` = @language, `license_key` = @license_key, `html_url` = @html_url, `api_url` = @api_url, `updated_at` = @updated_at;";
            }

            command.Parameters.AddWithValue("@id", repo.Id);
            command.Parameters.AddWithValue("@node_id", repo.NodeId);
            command.Parameters.AddWithValue("@owner_id", repo.OwnerId);
            command.Parameters.AddWithValue("@name", repo.Name);
            command.Parameters.AddWithValue("@description", repo.Description);
            command.Parameters.AddWithValue("@language", repo.Language);
            command.Parameters.AddWithValue("@license_key", repo.LicenseKey);
            command.Parameters.AddWithValue("@subscribers_count", repo.SubscribersCount);
            command.Parameters.AddWithValue("@stargazers_count", repo.StargazersCount);
            command.Parameters.AddWithValue("@forks_count", repo.ForksCount);
            command.Parameters.AddWithValue("@fork", repo.Fork);
            command.Parameters.AddWithValue("@archived", repo.Archived);
            command.Parameters.AddWithValue("@html_url", repo.HtmlUrl);
            command.Parameters.AddWithValue("@api_url", repo.ApiUrl);
            command.Parameters.AddWithValue("@created_at", repo.CreatedAt);
            command.Parameters.AddWithValue("@updated_at", repo.UpdatedAt);
            command.Parameters.AddWithValue("@parent_id", repo.ParentId);
            command.Parameters.AddWithValue("@source_id", repo.SourceId);
            command.Parameters.AddWithValue("@fetched_at", repo.FetchedAt);
            command.Parameters.AddWithValue("@valid", repo.Valid);

            command.ExecuteNonQuery();

            //command.Parameters.Add("@id", MySqlDbType.Int64);
            //command.Parameters.Add("@node_id", MySqlDbType.VarChar, 64);
            //command.Parameters.Add("@owner_id", MySqlDbType.Int64);
            //command.Parameters.Add("@name", MySqlDbType.VarChar, 1024);
            //command.Parameters.Add("@description", MySqlDbType.VarChar, 1024);
            //command.Parameters.Add("@language", MySqlDbType.VarChar, 1024);
            //command.Parameters.Add("@license_key", MySqlDbType.VarChar, 128);
            //command.Parameters.Add("@subscribers_count", MySqlDbType.Int32);
            //command.Parameters.Add("@stargazers_count", MySqlDbType.Int32);
            //command.Parameters.Add("@forks_count", MySqlDbType.Int32);
            //command.Parameters.Add("@private", MySqlDbType.Bit);
            //command.Parameters.Add("@fork", MySqlDbType.Bit);
            //command.Parameters.Add("@archived", MySqlDbType.Bit);
            //command.Parameters.Add("@html_url", MySqlDbType.VarChar, 1024);
            //command.Parameters.Add("@api_url", MySqlDbType.VarChar, 1024);
            //command.Parameters.Add("@created_at", MySqlDbType.DateTime);
            //command.Parameters.Add("@parent_id", MySqlDbType.Int64);
            //command.Parameters.Add("@source_id", MySqlDbType.Int64);
        }

        public Repository Repository_SelectByName(string name)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`repository` WHERE `name` = @name";
            command.Parameters.AddWithValue("@name", name);

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                if (dataReader.Read())
                {
                    return ParseRepository(dataReader);
                }
                else
                {
                    return null;
                }
            }
        }

        public Repository Repository_SelectById(long id)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`repository` WHERE `id` = @id";
            command.Parameters.AddWithValue("@id", id);

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                if (dataReader.Read())
                {
                    return ParseRepository(dataReader);
                }
                else
                {
                    return null;
                }
            }
        }

        public List<Repository> Repository_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`repository` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            var repositories = new List<Repository>();
            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                while (dataReader.Read())
                {
                    repositories.Add(ParseRepository(dataReader));
                }
            }
            return repositories;
        }

        public List<Repository> Repositories_SelectMostStar(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_star_repository` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", limit);

            var mostStarRepos = new List<Repository>();
            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                while (dataReader.Read())
                {
                    mostStarRepos.Add(ParseRepository(dataReader));
                }
            }
            return mostStarRepos;
        }

        public List<Repository> Repositories_SelectMostSubscriber(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_subscriber_repository` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", limit);

            var mostStarRepos = new List<Repository>();
            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                while (dataReader.Read())
                {
                    mostStarRepos.Add(ParseRepository(dataReader));
                }
            }
            return mostStarRepos;
        }

        public List<Repository> Repositories_SelectMostFork(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_fork_repository` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", limit);

            var mostStarRepos = new List<Repository>();
            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                while (dataReader.Read())
                {
                    mostStarRepos.Add(ParseRepository(dataReader));
                }
            }
            return mostStarRepos;
        }
        #endregion

        private User ParseUser(MySqlDataReader dataReader)
        {
            return new User(
                            dataReader.GetInt64("id"), dataReader.GetString("node_id"), (Account.AccountType)dataReader.GetInt32("type_id"), dataReader.GetString("login"),
                            dataReader.GetInt32("following"), dataReader.GetInt32("followers"), dataReader.GetInt32("public_repos"), dataReader.GetString("avatar_url"),
                            dataReader.GetString("html_url"), dataReader.GetString("api_url"), dataReader.GetString("blog_url"), dataReader.GetString("email"),
                            dataReader.GetString("bio"), dataReader.GetString("company"), dataReader.GetString("location"), dataReader.GetDateTime("created_at"),
                            dataReader.GetDateTime("fetched_at"), dataReader.GetBoolean("valid"), dataReader.GetBoolean("site_admin"), dataReader.GetBoolean("suspended"),
                            dataReader.GetDateTime("suspended_at"), dataReader.GetDateTime("updated_at"));
        }

        private Organization ParseOrganization(MySqlDataReader dataReader)
        {
            return new Organization(
                            dataReader.GetInt64("id"), dataReader.GetString("node_id"), (Account.AccountType)dataReader.GetInt32("type_id"), dataReader.GetString("login"),
                            dataReader.GetInt32("following"), dataReader.GetInt32("followers"), dataReader.GetInt32("public_repos"), dataReader.GetString("avatar_url"),
                            dataReader.GetString("html_url"), dataReader.GetString("api_url"), dataReader.GetString("blog_url"), dataReader.GetString("email"),
                            dataReader.GetString("bio"), dataReader.GetString("company"), dataReader.GetString("location"), dataReader.GetDateTime("created_at"),
                            dataReader.GetDateTime("fetched_at"), dataReader.GetBoolean("valid"));
        }

        private GitHub_Data_Collector.License ParseLicense(MySqlDataReader dataReader)
        {
            return new GitHub_Data_Collector.License(
                dataReader.GetString("key"), dataReader.GetString("node_id"), dataReader.GetString("name"),
                dataReader.GetString("spdx_id"), dataReader.GetString("api_url"));
        }

        private Repository ParseRepository(MySqlDataReader dataReader)
        {
            return new Repository(
                dataReader.GetInt64("id"), dataReader.GetString("node_id"), dataReader.GetInt64("owner_id"), dataReader.GetString("name"),
                dataReader.GetString("description"), dataReader.GetString("language"), dataReader.GetString("license_key"), dataReader.GetInt32("subscribers_count"),
                dataReader.GetInt32("stargazers_count"), dataReader.GetInt32("forks_count"), dataReader.GetBoolean("fork"), dataReader.GetBoolean("archived"),
                dataReader.GetString("html_url"), dataReader.GetString("api_url"), dataReader.GetDateTime("created_at"), dataReader.GetDateTime("updated_at"),
                dataReader.GetInt64("parent_id"), dataReader.GetInt64("source_id"), dataReader.GetDateTime("fetched_at"), dataReader.GetBoolean("valid"));
        }
    }
}
