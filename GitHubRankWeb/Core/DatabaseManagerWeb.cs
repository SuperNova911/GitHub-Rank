using GitHubDataCollector;
using GitHubRankWeb.Models;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace GitHubRankWeb.Core
{
    public class DatabaseManagerWeb
    {
        private MySqlConnection connection = null;
        private Queue<MySqlCommand> commands = new Queue<MySqlCommand>();

        public bool Connected => connection != null && connection.State == ConnectionState.Open;

        private DatabaseManagerWeb()
        {

        }

        #region Singleton
        private static DatabaseManagerWeb instance = null;
        private static readonly object instanceLock = new object();

        public static DatabaseManagerWeb Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (instance == null)
                    {
                        instance = new DatabaseManagerWeb();
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
                        Reference = $"/account/user?login={login}"
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
                        Reference = $"/account/organization?login={login}"
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
                        Reference = $"/account/user?login={userLogin}"
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
                        Reference = $"/account/user?login={login}"
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
                        Reference = $"/account/organization?login={login}"
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
                        Reference = $"/account/user?login={login}"
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
                        Reference = $"/account/user?login={login}"
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
                        Reference = $"/account/organization?login={login}"
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
                        Reference = $"/account/user?login={login}"
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
                        Reference = $"/account/user?login={login}"
                    });
                }
                return items;
            }
        }

        public List<LanguageModel> MostLanguage_SelectAll(int limit)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_language` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using MySqlDataReader dataReader = command.ExecuteReader();
            int rank = 1;
            var languages = new List<LanguageModel>();
            while (dataReader.Read())
            {
                string name = dataReader.GetString("language");
                if (string.IsNullOrWhiteSpace(name))
                {
                    continue;
                }

                languages.Add(new LanguageModel()
                {
                    Rank = rank++,
                    Name = name,
                    Score = dataReader.GetInt32("language_count"),
                });
            }

            int total = languages.Select(e => e.Score).Sum();
            foreach (var language in languages)
            {
                language.Ratio = language.Score / (double)total * 100;
            }
            return languages;
        }

        public List<LicenseModel> MostLicense_SelectAll(int limit)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`most_license` LIMIT @limit";
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using MySqlDataReader dataReader = command.ExecuteReader();
            int rank = 1;
            var licenses = new List<LicenseModel>();
            while (dataReader.Read())
            {
                licenses.Add(new LicenseModel()
                {
                    Rank = rank++,
                    Name = dataReader.GetString("name"),
                    Score = dataReader.GetInt32("license_count"),
                });
            }

            int total = licenses.Select(e => e.Score).Sum();
            foreach (LicenseModel license in licenses)
            {
                license.Ratio = license.Score / (double)total * 100;
            }
            return licenses;
        }

        public User User_SelectByLogin(string login)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`user_account` WHERE `login` = @login;";
            command.Parameters.AddWithValue("@login", login);

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? ParseUser(dataReader) : null;
        }

        public Organization Organization_SelectByLogin(string login)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`organization_account` WHERE `login` = @login;";
            command.Parameters.AddWithValue("@login", login);

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? ParseOrganization(dataReader) : null;
        }

        public List<Repository> Repositories_SelectAllByOwnerLogin(string login, int limit)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT * FROM `github_rank`.`repository` AS `r` JOIN `github_rank`.`account` AS `a` ON `r`.`owner_id` = `a`.`id` " +
                "WHERE `a`.`login` = @login ORDER BY `stargazers_count` DESC LIMIT @limit;";
            command.Parameters.AddWithValue("@login", login);
            command.Parameters.AddWithValue("@limit", Math.Max(limit, 0));

            using MySqlDataReader dataReader = command.ExecuteReader();
            var repositories = new List<Repository>();
            while (dataReader.Read())
            {
                repositories.Add(ParseRepository(dataReader));
            }
            return repositories;
        }

        public int Account_TotalStar(string login)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT sum(r.stargazers_count) AS `total_star_count` FROM `github_rank`.`account` AS `a` JOIN `github_rank`.`repository` AS `r` ON `a`.`id` = `r`.`owner_id` " +
                "WHERE `a`.`login` = @login GROUP BY `a`.`id`;";
            command.Parameters.AddWithValue("@login", login);

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? dataReader.GetInt32("total_star_count") : 0;

        }

        public int Account_TotalFork(string login)
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT sum(r.forks_count) AS `total_fork_count` FROM `github_rank`.`account` AS `a` JOIN `github_rank`.`repository` AS `r` ON `a`.`id` = `r`.`owner_id` " +
                "WHERE `a`.`login` = @login GROUP BY `a`.`id`;";
            command.Parameters.AddWithValue("@login", login);

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? dataReader.GetInt32("total_fork_count") : 0;

        }

        public int Account_CountAll()
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT count(*) AS `count` FROM `github_rank`.`account`";

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? dataReader.GetInt32("count") : 0;
        }

        public int Repository_CountAll()
        {
            using var command = new MySqlCommand();
            command.Connection = connection;
            command.CommandText = "SELECT count(*) AS `count` FROM `github_rank`.`repository`";

            using MySqlDataReader dataReader = command.ExecuteReader();
            return dataReader.Read() ? dataReader.GetInt32("count") : 0;
        }

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

        private GitHubDataCollector.License ParseLicense(MySqlDataReader dataReader)
        {
            return new GitHubDataCollector.License(
                dataReader.GetString("key"), dataReader.GetString("node_id"), dataReader.GetString("name"),
                dataReader.GetString("spdx_id"), dataReader.GetString("api_url"));
        }

        private Repository ParseRepository(MySqlDataReader dataReader)
        {
            return new Repository(
                dataReader.GetInt64("id"), dataReader.GetString("node_id"), dataReader.GetInt64("owner_id"), dataReader.GetString("name"),
                dataReader.GetString("description"), dataReader.GetString("language"), dataReader.IsDBNull("license_key") ? null : dataReader.GetString("license_key"), dataReader.GetInt32("subscribers_count"),
                dataReader.GetInt32("stargazers_count"), dataReader.GetInt32("forks_count"), dataReader.GetBoolean("fork"), dataReader.GetBoolean("archived"),
                dataReader.GetString("html_url"), dataReader.GetString("api_url"), dataReader.GetDateTime("created_at"), dataReader.GetDateTime("updated_at"),
                dataReader.GetInt64("parent_id"), dataReader.GetInt64("source_id"), dataReader.GetDateTime("fetched_at"), dataReader.GetBoolean("valid"));
        }
    }
}
