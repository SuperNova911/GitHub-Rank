using Octokit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GitHubDataCollector
{
    public class ActionSet
    {
        public void User_SearchMostRepo(int pageFrom, int pageTo)
        {
            pageFrom = Math.Max(pageFrom, 1);
            pageTo = Math.Clamp(pageTo, pageFrom, 10);
            for (int page = pageFrom; page <= pageTo; page++)
            {
                Console.WriteLine($"Search most repo users, page: {page}");
                IReadOnlyList<Octokit.User> o_mostRepoUsers = GitHubAPI.Instance.Users_SearchMostRepo(page);
                if (o_mostRepoUsers.Count == 0)
                {
                    Console.WriteLine($"Search result is empty, skip page: {page}");
                    continue;
                }

                Console.WriteLine($"Save invalid user to DB");
                IEnumerable<User> users = o_mostRepoUsers.Select(o_user => new User(o_user, false));
                foreach (User user in users)
                {
                    DatabaseManager.Instance.Account_InsertOrUpdate(user);
                }
            }
        }

        public void User_UpdateInvalid(int limit, int parallelSize)
        {
            Console.WriteLine($"Update invalid users, {nameof(limit)}: {Math.Max(limit, 0)}");
            var updatedUsers = new List<User>();
            try
            {
                List<string> userLogins = DatabaseManager.Instance.UserLogin_SelectInvalidAll(limit);
                if (userLogins.Count == 0)
                {
                    return;
                }

                if (parallelSize > 1)
                {
                    foreach (List<string> logins in userLogins.Split(parallelSize))
                    {
                        Console.WriteLine($"Get {logins.Count} users");
                        Octokit.User[] o_users = GitHubAPI.Instance.Users_GetByLogin(logins);
                        if (o_users == null)
                        {
                            Console.WriteLine($"Get result is null, skip {logins.Count} users");
                            continue;
                        }

                        updatedUsers.AddRange(o_users.Select(o_user => new User(o_user, true)));
                    }
                }
                else
                {
                    foreach (string login in userLogins)
                    {
                        Console.WriteLine($"Get user: {login}");
                        Octokit.User o_user = GitHubAPI.Instance.User_GetByLogin(login);
                        if (o_user == null)
                        {
                            Console.WriteLine($"Get result is null, skip user: {login}");
                            continue;
                        }

                        updatedUsers.Add(new User(o_user, true));
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.GetType().Name} detected, jump to DB stage");
                Console.WriteLine(e);
            }

            Console.WriteLine($"Save updated users to DB, count: {updatedUsers.Count}");
            DatabaseManager.Instance.User_InsertOrUpdate(updatedUsers);
        }

        public void User_Fetch(string userLogin)
        {
            Octokit.User o_user = GitHubAPI.Instance.User_GetByLogin(userLogin);
            if (o_user == null)
            {
                Console.WriteLine($"No match user: {userLogin}");
                return;
            }

            var user = new User(o_user, true);
            DatabaseManager.Instance.Account_InsertOrUpdate(user);

            var o_userRepos = GitHubAPI.Instance.Repositories_GetAllForUser(userLogin);
            if (o_userRepos == null)
            {

            }
        }

        public void User_GetAllFollower(int userLimit, int followerLimit)
        {
            Console.WriteLine($"Get users follower, {nameof(userLimit)}: {userLimit}, {nameof(followerLimit)}: {followerLimit}");
            List<string> userLogins = DatabaseManager.Instance.UserLogin_SelectNeedFollowerSearch(userLimit);
            foreach (string userLogin in userLogins)
            {
                Console.Write($"Get all followers for user: {userLogin}\t");
                var o_followers = GitHubAPI.Instance.Users_GetAllFollower(userLogin, followerLimit, 1);
                if (o_followers == null)
                {
                    continue;
                }

                List<User> followers = o_followers
                    .Where(o_user => AccountCollection.Instance.AccountIds.Contains(o_user.Id) == false)
                    .Select(o_user => new User(o_user, false))
                    .ToList();

                Console.WriteLine($"Save {o_followers.Count} followers");
                DatabaseManager.Instance.User_InsertOrUpdate(followers);
                DatabaseManager.Instance.UserLogin_MarkFollowerSearched(userLogin, followers.Count);
            }
        }

        public void Organization_UpdateInvalid(int limit)
        {
            Console.WriteLine($"Update invalid organizations, {nameof(limit)}: {limit}");
            var updatedOrgs = new List<Organization>();

            try
            {
                List<Organization> invalidOrgs = DatabaseManager.Instance.Organization_SelectInvalidAll(limit);
                if (invalidOrgs.Count == 0)
                {
                    return;
                }

                foreach (Organization org in invalidOrgs)
                {
                    Console.WriteLine($"Get organization: {org.Login}");
                    Octokit.Organization o_org = GitHubAPI.Instance.Organization_GetByLogin(org.Login);
                    if (o_org == null)
                    {
                        Console.WriteLine($"Get result is null, skip organization: {org.Login}");
                        continue;
                    }

                    updatedOrgs.Add(new Organization(o_org, true));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.GetType().Name} detected, jump to DB stage");
                Console.WriteLine(e);
            }

            Console.WriteLine($"Save updated organizations to DB, count: {updatedOrgs.Count}");
            DatabaseManager.Instance.Organization_InsertOrUpdate(updatedOrgs);
        }

        public void Organization_SearchMostRepo(int pageFrom, int pageTo)
        {
            pageFrom = Math.Max(pageFrom, 1);
            pageTo = Math.Clamp(pageTo, pageFrom, 10);
            for (int page = pageFrom; page <= pageTo; page++)
            {
                Console.WriteLine($"Search most repo organizations, page: {page}");
                IReadOnlyList<Octokit.User> o_mostRepoOrgs = GitHubAPI.Instance.Organizations_SearchMostRepo(page);
                if (o_mostRepoOrgs.Count == 0)
                {
                    Console.WriteLine($"Search result is empty, skip page: {page}");
                    continue;
                }

                Console.WriteLine($"Save invalid organizations to DB");
                IEnumerable<Organization> orgs = o_mostRepoOrgs.Select(o_org => new Organization(o_org, false));
                foreach (Organization org in orgs)
                {
                    DatabaseManager.Instance.Account_InsertOrUpdate(org);
                }
            }
        }

        public void Repository_UpdateMostStar(int pageFrom, int pageTo)
        {
            var repoIds = new HashSet<long>();
            var userLogins = new HashSet<string>();
            var orgLogins = new HashSet<string>();
            var licenses = new HashSet<License>();

            pageFrom = Math.Max(pageFrom, 1);
            pageTo = Math.Clamp(pageTo, pageFrom, 10);
            for (int page = pageFrom; page <= pageTo; page++)
            {
                try
                {
                    Console.WriteLine($"Search MostStarRepositories Page: {page}");
                    IReadOnlyList<Octokit.Repository> o_mostStarRepos = GitHubAPI.Instance.Repositories_SearchMostStar(page);
                    Console.WriteLine();
                    if (o_mostStarRepos.Count == 0)
                    {
                        continue;
                    }

                    repoIds.Clear();
                    userLogins.Clear();
                    orgLogins.Clear();
                    licenses.Clear();

                    foreach (var o_repo in o_mostStarRepos)
                    {
                        repoIds.Add(o_repo.Id);
                        var owner = o_repo.Owner;
                        if ((owner.Type.HasValue && (owner.Type.Value == AccountType.User || owner.Type.Value != AccountType.Organization)) || owner.Type.HasValue == false)
                        {
                            userLogins.Add(owner.Login);
                        }
                        else
                        {
                            orgLogins.Add(owner.Login);
                        }

                        if (o_repo.License != null && LicenseCollection.Instance.LicenseDictionary.ContainsKey(o_repo.License.Key) == false)
                        {
                            licenses.Add(new License(o_repo.License));
                        }
                    }

                    foreach (License license in licenses)
                    {
                        Console.WriteLine($"Update license: {license.Key}");
                        DatabaseManager.Instance.Lisence_InsertOrUpdate(license);
                    }
                    LicenseCollection.Instance.UpdateCollectionFromDB();

                    Console.WriteLine("Request UsersByLogin");
                    Octokit.User[] o_users = GitHubAPI.Instance.Users_GetByLogin(userLogins);
                    if (o_users == null)
                    {
                        continue;
                    }
                    foreach (User user in o_users.Select(o_user => new User(o_user, true)))
                    {
                        DatabaseManager.Instance.Account_InsertOrUpdate(user);
                    }

                    Console.WriteLine("Request OrgsByLogin");
                    Octokit.Organization[] o_orgs = GitHubAPI.Instance.Organizations_GetByLogin(orgLogins);
                    if (o_orgs == null)
                    {
                        continue;
                    }
                    foreach (Organization organization in o_orgs.Select(o_org => new Organization(o_org, true)))
                    {
                        DatabaseManager.Instance.Account_InsertOrUpdate(organization);
                    }

                    Console.WriteLine("Request ReposById");
                    Octokit.Repository[] o_repos = GitHubAPI.Instance.Repositories_GetById(repoIds);
                    if (o_repos == null)
                    {
                        continue;
                    }
                    foreach (Repository repository in o_repos.Select(o_repo => new Repository(o_repo, true)))
                    {
                        DatabaseManager.Instance.Repository_InsertOrUpdate(repository);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Skip page: {page}");
                    Console.WriteLine(e);
                }
            }
        }

        public void Repository_UpdateMostFork(int pageFrom, int pageTo)
        {
            var repoIds = new HashSet<long>();
            var userLogins = new HashSet<string>();
            var orgLogins = new HashSet<string>();
            var licenses = new HashSet<License>();

            pageFrom = Math.Max(pageFrom, 1);
            pageTo = Math.Clamp(pageTo, pageFrom, 10);
            for (int page = pageFrom; page <= pageTo; page++)
            {
                try
                {
                    IReadOnlyList<Octokit.Repository> o_mostForkRepos = GitHubAPI.Instance.Repositories_SearchMostFork(page);
                    if (o_mostForkRepos.Count == 0)
                    {
                        continue;
                    }

                    repoIds.Clear();
                    userLogins.Clear();
                    orgLogins.Clear();
                    licenses.Clear();

                    foreach (var o_repo in o_mostForkRepos)
                    {
                        repoIds.Add(o_repo.Id);
                        var owner = o_repo.Owner;
                        if ((owner.Type.HasValue && (owner.Type.Value == AccountType.User || owner.Type.Value != AccountType.Organization)) || owner.Type.HasValue == false)
                        {
                            userLogins.Add(owner.Login);
                        }
                        else
                        {
                            orgLogins.Add(owner.Login);
                        }

                        if (o_repo.License != null && LicenseCollection.Instance.LicenseDictionary.ContainsKey(o_repo.License.Key) == false)
                        {
                            licenses.Add(new License(o_repo.License));
                        }
                    }

                    foreach (License license in licenses)
                    {
                        DatabaseManager.Instance.Lisence_InsertOrUpdate(license);
                    }
                    LicenseCollection.Instance.UpdateCollectionFromDB();

                    Octokit.User[] o_users = GitHubAPI.Instance.Users_GetByLogin(userLogins);
                    if (o_users == null)
                    {
                        continue;
                    }
                    foreach (User user in o_users.Select(o_user => new User(o_user, true)))
                    {
                        DatabaseManager.Instance.Account_InsertOrUpdate(user);
                    }

                    Octokit.Organization[] o_orgs = GitHubAPI.Instance.Organizations_GetByLogin(orgLogins);
                    if (o_orgs == null)
                    {
                        continue;
                    }
                    foreach (Organization organization in o_orgs.Select(o_org => new Organization(o_org, true)))
                    {
                        DatabaseManager.Instance.Account_InsertOrUpdate(organization);
                    }

                    Octokit.Repository[] o_repos = GitHubAPI.Instance.Repositories_GetById(repoIds);
                    if (o_repos == null)
                    {
                        continue;
                    }
                    foreach (Repository repository in o_repos.Select(o_repo => new Repository(o_repo, true)))
                    {
                        DatabaseManager.Instance.Repository_InsertOrUpdate(repository);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Skip page: {page}");
                    Console.WriteLine(e);
                }
            }
        }

        public void Repository_UpdateUsersRepo(int limit, int pageLimit)
        {
            Console.WriteLine($"Update users repository, {nameof(limit)}: {limit}, {nameof(pageLimit)}: {pageLimit}");
            List<string> userLogins = DatabaseManager.Instance.UserLogin_SelectNeedRepoUpdate(limit, pageLimit * 100);
            foreach (string userLogin in userLogins)
            {
                Console.Write($"Get all repos for user: {userLogin}\t");
                IReadOnlyList<Octokit.Repository> o_repos = GitHubAPI.Instance.Repositories_GetAllForUser(userLogin, pageLimit);
                if (o_repos == null)
                {
                    Console.WriteLine($"Get result is empty, skip user: {userLogin}");
                    continue;
                }

                Console.WriteLine($"Fetched {o_repos.Count} repos");
                DatabaseManager.Instance.Repository_InsertOrUpdate(o_repos.Select(o_repo => new Repository(o_repo, true)).ToList());
            }
        }

        public void Repository_UpdateOrgsRepo(int limit, int pageLimit)
        {
            Console.WriteLine($"Update organizations repository, {nameof(limit)}: {limit}, {nameof(pageLimit)}: {pageLimit}");
            List<string> orgLogins = DatabaseManager.Instance.OrganizationLogin_SelectNeedRepoUpdate(limit, pageLimit * 100);
            foreach (string orgLogin in orgLogins)
            {
                Console.Write($"Get all repos for organization: {orgLogin}\t");
                IReadOnlyList<Octokit.Repository> o_repos = GitHubAPI.Instance.Repositories_GetAllForOrg(orgLogin, pageLimit);
                if (o_repos == null)
                {
                    Console.WriteLine($"Get result is empty, skip organization: {orgLogin}");
                    continue;
                }

                Console.WriteLine($"Fetched {o_repos.Count} repos");
                DatabaseManager.Instance.Repository_InsertOrUpdate(o_repos.Select(o_repo => new Repository(o_repo, true)).ToList());
            }
        }
    }
}