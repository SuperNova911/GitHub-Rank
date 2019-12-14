using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace GitHubDataCollector
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

        public void User_InsertOrUpdate(List<User> users)
        {
            if (users == null || users.Count == 0)
            {
                return;
            }

            foreach (List<User> splitedUsers in users.Split(100))
            {
                using var command = new MySqlCommand();
                command.Connection = connection;

                var commandText = new StringBuilder();
                for (int i = 0; i < splitedUsers.Count; i++)
                {
                    User user = splitedUsers[i];
                    if (user.Valid)
                    {
                        commandText.Append("INSERT INTO `github_rank`.`account` (`id`, `node_id`, `type_id`, `login`, `following`, `followers`, `public_repos`, `avatar_url`, `html_url`, `api_url`, `blog_url`, `email`, `bio`, `company`, `location`, `created_at`, `fetched_at`, `valid`) " +
                            $"VALUES (@id{i}, @node_id{i}, @type_id{i}, @login{i}, @following{i}, @followers{i}, @public_repos{i}, @avatar_url{i}, @html_url{i}, @api_url{i}, @blog_url{i}, @email{i}, @bio{i}, @company{i}, @location{i}, @created_at{i}, @fetched_at{i}, @valid{i}) " +
                            $"ON DUPLICATE KEY UPDATE `type_id` = @type_id{i}, `login` = @login{i}, `following` = @following{i}, `followers` = @followers{i}, `public_repos` = @public_repos{i}, `avatar_url` = @avatar_url{i}, `html_url` = @html_url{i}, `api_url` = @api_url{i}, `blog_url` = @blog_url{i}, `email` = @email{i}, `bio` = @bio{i}, `company` = @company{i}, `location` = @location{i}, `created_at` = @created_at{i}, `fetched_at` = @fetched_at{i}, `valid` = @valid{i};" +
                            "INSERT INTO `github_rank`.`user` (`account_id`, `site_admin`, `suspended`, `suspended_at`, `updated_at`) " +
                            $"VALUES (@account_id{i}, @site_admin{i}, @suspended{i}, @suspended_at{i}, @updated_at{i})" +
                            $"ON DUPLICATE KEY UPDATE `site_admin` = @site_admin{i}, `suspended` = @suspended{i}, `suspended_at` = @suspended_at{i}, `updated_at` = @updated_at{i};");
                    }
                    else
                    {
                        commandText.Append("INSERT INTO `github_rank`.`account` (`id`, `node_id`, `type_id`, `login`, `following`, `followers`, `public_repos`, `avatar_url`, `html_url`, `api_url`, `blog_url`, `email`, `bio`, `company`, `location`, `created_at`, `fetched_at`, `valid`) " +
                            $"VALUES (@id{i}, @node_id{i}, @type_id{i}, @login{i}, @following{i}, @followers{i}, @public_repos{i}, @avatar_url{i}, @html_url{i}, @api_url{i}, @blog_url{i}, @email{i}, @bio{i}, @company{i}, @location{i}, @created_at{i}, @fetched_at{i}, @valid{i}) " +
                            $"ON DUPLICATE KEY UPDATE `type_id` = @type_id{i}, `login` = @login{i}, `avatar_url` = @avatar_url{i}, `html_url` = @html_url{i}, `api_url` = @api_url{i};" +
                            "INSERT INTO `github_rank`.`user` (`account_id`, `site_admin`, `suspended`, `suspended_at`, `updated_at`) " +
                            $"VALUES (@account_id{i}, @site_admin{i}, @suspended{i}, @suspended_at{i}, @updated_at{i}) " +
                            $"ON DUPLICATE KEY UPDATE `account_id` = @account_id{i};");
                    }

                    command.Parameters.AddWithValue($"@id{i}", user.Id);
                    command.Parameters.AddWithValue($"@node_id{i}", user.NodeId);
                    command.Parameters.AddWithValue($"@type_id{i}", user.TypeId);
                    command.Parameters.AddWithValue($"@login{i}", user.Login);
                    command.Parameters.AddWithValue($"@following{i}", user.Following);
                    command.Parameters.AddWithValue($"@followers{i}", user.Followers);
                    command.Parameters.AddWithValue($"@public_repos{i}", user.PublicRepos);
                    command.Parameters.AddWithValue($"@avatar_url{i}", user.AvatarUrl);
                    command.Parameters.AddWithValue($"@html_url{i}", user.HtmlUrl);
                    command.Parameters.AddWithValue($"@api_url{i}", user.ApiUrl);
                    command.Parameters.AddWithValue($"@blog_url{i}", user.BlogUrl);
                    command.Parameters.AddWithValue($"@email{i}", user.Email);
                    command.Parameters.AddWithValue($"@bio{i}", user.Bio);
                    command.Parameters.AddWithValue($"@company{i}", user.Company);
                    command.Parameters.AddWithValue($"@location{i}", user.Location);
                    command.Parameters.AddWithValue($"@created_at{i}", user.CreatedAt);
                    command.Parameters.AddWithValue($"@fetched_at{i}", user.FetchedAt);
                    command.Parameters.AddWithValue($"@valid{i}", user.Valid);

                    command.Parameters.AddWithValue($"@account_id{i}", user.Id);
                    command.Parameters.AddWithValue($"@site_admin{i}", user.SiteAdmin);
                    command.Parameters.AddWithValue($"@suspended{i}", user.Suspended);
                    command.Parameters.AddWithValue($"@suspended_at{i}", user.SuspendedAt);
                    command.Parameters.AddWithValue($"@updated_at{i}", user.UpdatedAt);
                }
                command.CommandText = commandText.ToString();

                command.ExecuteNonQuery();
            }
        }

        public void Organization_InsertOrUpdate(List<Organization> organizations)
        {
            if (organizations == null || organizations.Count == 0)
            {
                return;
            }

            foreach (List<Organization> orgs in organizations.Split(100))
            {
                using var command = new MySqlCommand();
                command.Connection = connection;

                var commandText = new StringBuilder();
                for (int i = 0; i < orgs.Count; i++)
                {
                    var organization = orgs[i];
                    if (organization.Valid)
                    {
                        commandText.Append("INSERT INTO `github_rank`.`account` (`id`, `node_id`, `type_id`, `login`, `following`, `followers`, `public_repos`, `avatar_url`, `html_url`, `api_url`, `blog_url`, `email`, `bio`, `company`, `location`, `created_at`, `fetched_at`, `valid`) " +
                            $"VALUES (@id{i}, @node_id{i}, @type_id{i}, @login{i}, @following{i}, @followers{i}, @public_repos{i}, @avatar_url{i}, @html_url{i}, @api_url{i}, @blog_url{i}, @email{i}, @bio{i}, @company{i}, @location{i}, @created_at{i}, @fetched_at{i}, @valid{i}) " +
                            $"ON DUPLICATE KEY UPDATE `type_id` = @type_id{i}, `login` = @login{i}, `following` = @following{i}, `followers` = @followers{i}, `public_repos` = @public_repos{i}, `avatar_url` = @avatar_url{i}, `html_url` = @html_url{i}, `api_url` = @api_url{i}, `blog_url` = @blog_url{i}, `email` = @email{i}, `bio` = @bio{i}, `company` = @company{i}, `location` = @location{i}, `created_at` = @created_at{i}, `fetched_at` = @fetched_at{i}, `valid` = @valid{i};");
                    }
                    else
                    {
                        commandText.Append("INSERT INTO `github_rank`.`account` (`id`, `node_id`, `type_id`, `login`, `following`, `followers`, `public_repos`, `avatar_url`, `html_url`, `api_url`, `blog_url`, `email`, `bio`, `company`, `location`, `created_at`, `fetched_at`, `valid`) " +
                            $"VALUES (@id{i}, @node_id{i}, @type_id{i}, @login{i}, @following{i}, @followers{i}, @public_repos{i}, @avatar_url{i}, @html_url{i}, @api_url{i}, @blog_url{i}, @email{i}, @bio{i}, @company{i}, @location{i}, @created_at{i}, @fetched_at{i}, @valid{i}) " +
                            $"ON DUPLICATE KEY UPDATE `type_id` = @type_id{i}, `login` = @login{i}, `avatar_url` = @avatar_url{i}, `html_url` = @html_url{i}, `api_url` = @api_url{i};");
                    }
                    commandText.Append("INSERT INTO `github_rank`.`organization` (`account_id`) " +
                        $"VALUES (@account_id{i}) " +
                        $"ON DUPLICATE KEY UPDATE `account_id` = @account_id{i};");

                    command.Parameters.AddWithValue($"@id{i}", organization.Id);
                    command.Parameters.AddWithValue($"@node_id{i}", organization.NodeId);
                    command.Parameters.AddWithValue($"@type_id{i}", organization.TypeId);
                    command.Parameters.AddWithValue($"@login{i}", organization.Login);
                    command.Parameters.AddWithValue($"@following{i}", organization.Following);
                    command.Parameters.AddWithValue($"@followers{i}", organization.Followers);
                    command.Parameters.AddWithValue($"@public_repos{i}", organization.PublicRepos);
                    command.Parameters.AddWithValue($"@avatar_url{i}", organization.AvatarUrl);
                    command.Parameters.AddWithValue($"@html_url{i}", organization.HtmlUrl);
                    command.Parameters.AddWithValue($"@api_url{i}", organization.ApiUrl);
                    command.Parameters.AddWithValue($"@blog_url{i}", organization.BlogUrl);
                    command.Parameters.AddWithValue($"@email{i}", organization.Email);
                    command.Parameters.AddWithValue($"@bio{i}", organization.Bio);
                    command.Parameters.AddWithValue($"@company{i}", organization.Company);
                    command.Parameters.AddWithValue($"@location{i}", organization.Location);
                    command.Parameters.AddWithValue($"@created_at{i}", organization.CreatedAt);
                    command.Parameters.AddWithValue($"@fetched_at{i}", organization.FetchedAt);
                    command.Parameters.AddWithValue($"@valid{i}", organization.Valid);

                    command.Parameters.AddWithValue($"@account_id{i}", organization.Id);
                }
                command.CommandText = commandText.ToString();

                command.ExecuteNonQuery();
            }
        }

        public int User_Count()
        {
            using (var command = new MySqlCommand())
            {
                command.Connection = connection;
                command.CommandText = "SELECT count(*) AS `user_count` FROM `github_rank`.`account` WHERE `type_id` = 0;";
                
                using (MySqlDataReader dataReader = command.ExecuteReader())
                {
                    return dataReader.Read() ? dataReader.GetInt32("user_count") : 0;
                }
            }
        }

        public int Organization_Count()
        {
            using (var command = new MySqlCommand())
            {
                command.Connection = connection;
                command.CommandText = "SELECT count(*) AS `organization_count` FROM `github_rank`.`account` WHERE `type_id` = 1;";

                using (MySqlDataReader dataReader = command.ExecuteReader())
                {
                    return dataReader.Read() ? dataReader.GetInt32("organization_count") : 0;
                }
            }
        }

        public int Account_CountInvalid()
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT count(*) AS `invalid_account_count` FROM `github_rank`.`account` WHERE `valid` = FALSE;";

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? dataReader.GetInt32("invalid_account_count") : 0;
        }

        public List<long> AccountId_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`account` WHERE `valid` = TRUE LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var ids = new List<long>();
                while (dataReader.Read())
                {
                    ids.Add(dataReader.GetInt64("id"));
                }
                return ids;
            }
        }

        public List<string> UserLogin_SelectInvalidAll(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`user_account_invalid` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var logins = new List<string>();
                while (dataReader.Read())
                {
                    logins.Add(dataReader.GetString("login"));
                }
                return logins;
            }
        }

        public void UserLogin_MarkFollowerSearched(string userLogin, int searchedCount)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "INSERT INTO `github_rank`.`user_login_follower_searched` (`login`, `updated_at`, `searched_count`)" +
                "VALUES (@login, @updated_at, @searched_count)" +
                "ON DUPLICATE KEY UPDATE `updated_at` = @updated_at, `searched_count` = @searched_count";
            command.Parameters.AddWithValue("@login", userLogin);
            command.Parameters.AddWithValue("@updated_at", DateTime.Now);
            command.Parameters.AddWithValue("@searched_count", searchedCount);

            command.ExecuteNonQuery();
        }

        public List<string> UserLogin_SelectNeedFollowerSearch(int limit)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`user_login_need_follower_update` LIMIT @limit";
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

        public List<string> UserLogin_SelectNeedRepoUpdate(int limit, int maximumRepoCount)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`need_repo_update_user` ORDER BY `fetched_public_repo_count` ASC LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var userLogins = new List<string>();
                while (dataReader.Read())
                {
                    if (maximumRepoCount <= dataReader.GetInt32("fetched_public_repo_count"))
                    {
                        continue;
                    }
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

        public List<string> OrganizationLogin_SelectNeedRepoUpdate(int limit, int maximumRepoCount)
        {
            var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`need_repo_update_organization` ORDER BY `fetched_public_repo_count` ASC LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var orgLogins = new List<string>();
                while (dataReader.Read())
                {
                    if (maximumRepoCount <= dataReader.GetInt32("fetched_public_repo_count"))
                    {
                        continue;
                    }
                    orgLogins.Add(dataReader.GetString("login"));
                }
                return orgLogins;
            }
        }

        public User User_SelectByLogin(string login)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`user_account` WHERE `login` = @login;";

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? ParseUser(dataReader) : null;
        }

        public Organization Organization_SelectByLogin(string login)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`organization_account` WHERE `login` = @login;";

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? ParseOrganization(dataReader) : null;
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
                    "ON DUPLICATE KEY UPDATE `account_id` = @account_id;";
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
        public void Lisence_InsertOrUpdate(License license)
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

        public List<License> License_SelectAll(int limit)
        {
            var command = new MySqlCommand();
            command.CommandText = "SELECT * FROM `github_rank`.`license` LIMIT @limit";
            command.Connection = connection;
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using (MySqlDataReader dataReader = command.ExecuteReader())
            {
                var licenses = new List<License>();
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
        }

        public void Repository_InsertOrUpdate(List<Repository> repositories)
        {
            if (repositories == null || repositories.Count == 0)
            {
                return;
            }

            foreach (List<Repository> repos in repositories.Split(100))
            {
                using var command = new MySqlCommand();
                command.Connection = connection;

                var commandText = new StringBuilder();
                for (int i = 0; i < repos.Count; i++)
                {
                    Repository repo = repos[i];
                    if (repo.Valid)
                    {
                        commandText.Append("INSERT INTO `github_rank`.`repository` (`id`, `node_id`, `owner_id`, `name`, `description`, `language`, `license_key`, `subscribers_count`, `stargazers_count`, `forks_count`, `fork`, `archived`, `html_url`, `api_url`, `created_at`, `updated_at`, `parent_id`, `source_id`, `fetched_at`, `valid`)" +
                            $"VALUES (@id{i}, @node_id{i}, @owner_id{i}, @name{i}, @description{i}, @language{i}, @license_key{i}, @subscribers_count{i}, @stargazers_count{i}, @forks_count{i}, @fork{i}, @archived{i}, @html_url{i}, @api_url{i}, @created_at{i}, @updated_at{i}, @parent_id{i}, @source_id{i}, @fetched_at{i}, @valid{i}) " +
                            $"ON DUPLICATE KEY UPDATE `owner_id` = @owner_id{i}, `name` = @name{i}, `description` = @description{i}, `language` = @language{i}, `license_key` = @license_key{i}, `subscribers_count` = @subscribers_count{i}, `stargazers_count` = @stargazers_count{i}, `forks_count` = @forks_count{i}, `fork` = @fork{i}, `archived` = @archived{i}, `html_url` = @html_url{i}, `api_url` = @api_url{i}, `created_at` = @created_at{i}, `updated_at` = @updated_at{i}, `fetched_at` = @fetched_at{i}, `valid` = @valid{i};");
                    }
                    else
                    {
                        commandText.Append("INSERT INTO `github_rank`.`repository` (`id`, `node_id`, `owner_id`, `name`, `description`, `language`, `license_key`, `subscribers_count`, `stargazers_count`, `forks_count`, `fork`, `archived`, `html_url`, `api_url`, `created_at`, `updated_at`, `parent_id`, `source_id`, `fetched_at`, `valid`)" +
                            $"VALUES (@id{i}, @node_id{i}, @owner_id{i}, @name{i}, @description{i}, @language{i}, @license_key{i}, @subscribers_count{i}, @stargazers_count{i}, @forks_count{i}, @fork{i}, @archived{i}, @html_url{i}, @api_url{i}, @created_at{i}, @updated_at{i}, @parent_id{i}, @source_id{i}, @fetched_at{i}, @valid{i})" +
                            $"ON DUPLICATE KEY UPDATE `owner_id` = @owner_id{i}, `name` = @name{i}, `description` = @description{i}, `language` = @language{i}, `license_key` = @license_key{i}, `html_url` = @html_url{i}, `api_url` = @api_url{i}, `updated_at` = @updated_at{i};");
                    }

                    command.Parameters.AddWithValue($"@id{i}", repo.Id);
                    command.Parameters.AddWithValue($"@node_id{i}", repo.NodeId);
                    command.Parameters.AddWithValue($"@owner_id{i}", repo.OwnerId);
                    command.Parameters.AddWithValue($"@name{i}", repo.Name);
                    command.Parameters.AddWithValue($"@description{i}", repo.Description);
                    command.Parameters.AddWithValue($"@language{i}", repo.Language);
                    command.Parameters.AddWithValue($"@license_key{i}", repo.LicenseKey);
                    command.Parameters.AddWithValue($"@subscribers_count{i}", repo.SubscribersCount);
                    command.Parameters.AddWithValue($"@stargazers_count{i}", repo.StargazersCount);
                    command.Parameters.AddWithValue($"@forks_count{i}", repo.ForksCount);
                    command.Parameters.AddWithValue($"@fork{i}", repo.Fork);
                    command.Parameters.AddWithValue($"@archived{i}", repo.Archived);
                    command.Parameters.AddWithValue($"@html_url{i}", repo.HtmlUrl);
                    command.Parameters.AddWithValue($"@api_url{i}", repo.ApiUrl);
                    command.Parameters.AddWithValue($"@created_at{i}", repo.CreatedAt);
                    command.Parameters.AddWithValue($"@updated_at{i}", repo.UpdatedAt);
                    command.Parameters.AddWithValue($"@parent_id{i}", repo.ParentId);
                    command.Parameters.AddWithValue($"@source_id{i}", repo.SourceId);
                    command.Parameters.AddWithValue($"@fetched_at{i}", repo.FetchedAt);
                    command.Parameters.AddWithValue($"@valid{i}", repo.Valid);
                }
                command.CommandText = commandText.ToString();

                command.ExecuteNonQuery();
            }
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

        public int Repository_Count()
        {
            using (var command = new MySqlCommand())
            {
                command.Connection = connection;
                command.CommandText = "SELECT count(*) AS `repository_count` FROM `github_rank`.`repository`;";

                using (MySqlDataReader dataReader = command.ExecuteReader())
                {
                    return dataReader.Read() ? dataReader.GetInt32("repository_count") : 0;
                }
            }
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

        private License ParseLicense(MySqlDataReader dataReader)
        {
            return new License(
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
