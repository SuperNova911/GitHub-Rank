using Octokit;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;

namespace GitHub_Data_Collector
{
    public class GitHubAPI
    {
        private readonly TimeSpan coreRequestDelay = TimeSpan.FromSeconds(1);
        private readonly TimeSpan searchRequestDelay = TimeSpan.FromSeconds(2);
        private readonly int maximumRequestRetry = 3;
        private DateTime lastCoreRequest = DateTime.MinValue;
        private DateTime lastSearchRequest = DateTime.MinValue;

        private GitHubClient gitHubClient;

        private GitHubAPI()
        {

        }

        public RateLimit CoreRateLimit { get; private set; }
        public RateLimit SearchRateLimit { get; private set; }

        #region Singleton
        private static GitHubAPI instance = null;
        private static readonly object instanceLock = new object();

        public static GitHubAPI Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (instance == null)
                    {
                        instance = new GitHubAPI();
                    }
                    return instance;
                }
            }
        }
        #endregion

        public void InitializeGitHubClient(Credentials credentials)
        {
            gitHubClient = new GitHubClient(new ProductHeaderValue("GitHub-Rank"));
            gitHubClient.Credentials = credentials;

            var limit = gitHubClient.Miscellaneous.GetRateLimits().Result;
            Console.WriteLine($"Core {limit.Resources.Core.Remaining}/{limit.Resources.Core.Limit} Reset: {limit.Resources.Core.Reset.LocalDateTime}");
            Console.WriteLine($"Search {limit.Resources.Search.Remaining}/{limit.Resources.Search.Limit} Reset: {limit.Resources.Search.Reset.LocalDateTime}");
            CoreRateLimit = limit.Resources.Core;
            SearchRateLimit = limit.Resources.Search;
            lastCoreRequest = DateTime.Now;
            lastSearchRequest = DateTime.Now;
        }

        public void Test()
        {
            //var a5 = gitHubClient.Activity.Starring.GetAllForUser("SuperNova911").Result;
            //var a6 = gitHubClient.Activity.Watching.GetAllForUser("SuperNova911").Result;
            //var i3 = gitHubClient.Issue.GetAllForRepository(45717250, new ApiOptions { PageSize = 100, PageCount = 10 }).Result;
            //var o1 = gitHubClient.Organization.Member.GetAll("google").Result;
            //var p1 = gitHubClient.PullRequest.GetAllForRepository("dotnet", "docs", new PullRequestRequest() { State = ItemStateFilter.All }, new ApiOptions { PageSize = 100, PageCount = 10 }).Result;     // closed pr
            //var r1 = gitHubClient.Repository.Branch.GetAll("SuperNova911", "UnityPUBG").Result;
            //var r2 = gitHubClient.Repository.Collaborator.GetAll("SuperNova911", "UnityPUBG").Result;
            //var r4 = gitHubClient.Repository.Commit.GetAll(45717250, new ApiOptions() { PageSize = 100, PageCount = 10 }).Result;     // test for big repo
            //var r5 = gitHubClient.Repository.Content.GetReadme("SuperNova911", "UnityPUBG").Result;
            //var r6 = gitHubClient.Repository.Content.GetReadmeHtml("SuperNova911", "UnityPUBG").Result;
            //var r8 = gitHubClient.Repository.GetAllContributors("SuperNova911", "UnityPUBG").Result;
            //var r9 = gitHubClient.Repository.GetAllLanguages(45717250).Result;
            //var r10 = gitHubClient.Repository.Statistics.GetCodeFrequency(45717250).Result;
            //var r11 = gitHubClient.Repository.Statistics.GetCommitActivity(45717250).Result;
            //var r12 = gitHubClient.Repository.Statistics.GetContributors(45717250).Result;    // limit 100
            //var r13 = gitHubClient.Repository.Statistics.GetParticipation(45717250).Result;     // what is participation
            //var r14 = gitHubClient.Repository.Statistics.GetPunchCard(45717250).Result;     // what is punchcard
            //var u2 = gitHubClient.User.Followers.GetAll("SuperNova911").Result;
            //var u3 = gitHubClient.User.Followers.GetAllFollowing("SuperNova911").Result;

            Console.WriteLine("Kappa");
        }

        #region Account
        public Octokit.User User_GetByLogin(string userLogin)
        {
            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    Octokit.User user = gitHubClient.User.Get(userLogin).Result;
                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;
                    return user;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public Octokit.User[] Users_GetByLogin(IEnumerable<string> userLogins)
        {
            WaitForNextCoreRequest(2);

            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    IEnumerable<Task<Octokit.User>> tasks = userLogins.Select(userLogin => gitHubClient.User.Get(userLogin));
                    var result = Task.WhenAll(tasks).Result;

                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;
                    return result;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public IReadOnlyList<Octokit.User> Users_SearchByName(string name)
        {
            WaitForNextSearchRequest();

            lastSearchRequest = DateTime.Now;
            var searchResult = gitHubClient.Search.SearchUsers(new SearchUsersRequest(name)
            {

            }).Result;
            SearchRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

            if (searchResult.IncompleteResults)
            {
                Console.WriteLine("IncompleteResults");
                return new List<Octokit.User>().AsReadOnly();
            }
            return searchResult.Items;
        }

        public IReadOnlyList<Octokit.User> Users_SearchMostRepo(int targetPage)
        {
            WaitForNextSearchRequest();

            for (int tryCount = 1; tryCount < maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastSearchRequest = DateTime.Now;
                    var apiConnection = new ApiConnection(gitHubClient.Connection);
                    var searchResult = apiConnection.Get<SearchUsersResult>(ApiUrls.SearchUsers(), new Dictionary<string, string>()
                    {
                        { "q", "repos:>100+type:User" },
                        { "page", targetPage.ToString() },
                        { "per_page", "100" },
                        { "sort", "repositories" },
                        { "o", "desc" },
                    }).Result;
                    SearchRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

                    if (searchResult.IncompleteResults)
                    {
                        Console.WriteLine("IncompleteResults");
                    }
                    else
                    {
                        return searchResult.Items;
                    }
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForSearch(tryCount, ae);
                }
            }

            return new List<Octokit.User>().AsReadOnly();
        }

        public Octokit.Organization Organization_GetByLogin(string organizationLogin)
        {
            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    Octokit.Organization organization = gitHubClient.Organization.Get(organizationLogin).Result;
                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;
                    return organization;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public Octokit.Organization[] Organizations_GetByLogin(IEnumerable<string> organizationLogins)
        {
            WaitForNextCoreRequest(2);

            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    IEnumerable<Task<Octokit.Organization>> tasks = organizationLogins.Select(orgLogin => gitHubClient.Organization.Get(orgLogin));
                    var result = Task.WhenAll(tasks).Result;

                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;
                    return result;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public IReadOnlyList<Octokit.User> Organizations_SearchMostRepo(int targetPage)
        {
            WaitForNextSearchRequest();

            for (int tryCount = 1; tryCount < maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastSearchRequest = DateTime.Now;
                    var apiConnection = new ApiConnection(gitHubClient.Connection);
                    var searchResult = apiConnection.Get<SearchUsersResult>(ApiUrls.SearchUsers(), new Dictionary<string, string>()
                    {
                        { "q", "repos:>100+type:Org" },
                        { "page", targetPage.ToString() },
                        { "per_page", "100" },
                        { "sort", "repositories" },
                        { "o", "desc" },
                    }).Result;
                    SearchRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

                    if (searchResult.IncompleteResults)
                    {
                        Console.WriteLine("IncompleteResults");
                    }
                    else
                    {
                        return searchResult.Items;
                    }
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForSearch(tryCount, ae);
                }
            }

            return new List<Octokit.User>().AsReadOnly();
        }
        #endregion

        #region Repository
        public Octokit.Repository Repository_GetById(long repositoryId)
        {
            lastCoreRequest = DateTime.Now;
            Octokit.Repository repository = gitHubClient.Repository.Get(repositoryId).Result;
            return repository;
        }

        public Octokit.Repository[] Repositories_GetById(IEnumerable<long> repositoryIds)
        {
            WaitForNextCoreRequest(2);

            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    IEnumerable<Task<Octokit.Repository>> tasks = repositoryIds.Select(repoId => gitHubClient.Repository.Get(repoId));
                    var result = Task.WhenAll(tasks).Result;

                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;
                    return result;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public IReadOnlyList<Octokit.Repository> Repositories_GetAllForUser(string userLogin, int pageLimit = 10)
        {
            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    IReadOnlyList<Octokit.Repository> o_userRepos = gitHubClient.Repository.GetAllForUser(userLogin, new ApiOptions()
                    {
                        PageSize = 100,
                        PageCount = pageLimit
                    }).Result;
                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

                    return o_userRepos;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public IReadOnlyList<Octokit.Repository>[] Repositories_GetAllForUser(IEnumerable<string> userLogins, int pageLimit = 10)
        {
            WaitForNextCoreRequest(2);

            var apiOptions = new ApiOptions() { PageSize = 100, PageCount = pageLimit };
            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    IEnumerable<Task<IReadOnlyList<Octokit.Repository>>> tasks = userLogins.Select(e => gitHubClient.Repository.GetAllForUser(e, apiOptions));
                    IReadOnlyList<Octokit.Repository>[] o_userRepos = Task.WhenAll(tasks).Result;
                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

                    return o_userRepos;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public IReadOnlyList<Octokit.Repository> Repositories_GetAllForOrg(string orgLogin, int pageLimit = 10)
        {
            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastCoreRequest = DateTime.Now;
                    IReadOnlyList<Octokit.Repository> o_orgRepos = gitHubClient.Repository.GetAllForOrg(orgLogin, new ApiOptions()
                    {
                        PageSize = 100,
                        PageCount = pageLimit
                    }).Result;
                    CoreRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

                    return o_orgRepos;
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForCore(tryCount, ae);
                }
            }

            return null;
        }

        public IReadOnlyList<Octokit.Repository> Repositories_SearchMostStar(int targetPage)
        {
            WaitForNextSearchRequest();

            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastSearchRequest = DateTime.Now;
                    var searchResult = gitHubClient.Search.SearchRepo(new SearchRepositoriesRequest()
                    {
                        Stars = Octokit.Range.GreaterThan(100),
                        Page = targetPage,
                        SortField = RepoSearchSort.Stars,
                        Order = SortDirection.Descending,
                    }).Result;
                    SearchRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

                    if (searchResult.IncompleteResults)
                    {
                        Console.WriteLine("IncompleteResults");
                    }
                    else
                    {
                        return searchResult.Items;
                    }
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForSearch(tryCount, ae);
                }
            }

            return new List<Octokit.Repository>().AsReadOnly();
        }

        public IReadOnlyList<Octokit.Repository> Repositories_SearchMostFork(int targetPage)
        {
            WaitForNextSearchRequest();

            for (int tryCount = 1; tryCount <= maximumRequestRetry; tryCount++)
            {
                try
                {
                    lastSearchRequest = DateTime.Now;
                    var searchResult = gitHubClient.Search.SearchRepo(new SearchRepositoriesRequest()
                    {
                        Forks = Octokit.Range.GreaterThan(100),
                        Page = targetPage,
                        SortField = RepoSearchSort.Forks,
                        Order = SortDirection.Descending,
                    }).Result;
                    SearchRateLimit = gitHubClient.GetLastApiInfo().RateLimit;

                    if (searchResult.IncompleteResults)
                    {
                        Console.WriteLine("IncompleteResults");
                    }
                    else
                    {
                        return searchResult.Items;
                    }
                }
                catch (AggregateException ae)
                {
                    HandleAggregateExceptionForSearch(tryCount, ae);
                }
            }

            return new List<Octokit.Repository>().AsReadOnly();
        }
        #endregion

        private void WaitForNextCoreRequest(int delayMultiply = 1)
        {
            if (DateTime.Now - lastCoreRequest < coreRequestDelay * delayMultiply)
            {
                Task.Delay((coreRequestDelay * delayMultiply) - (DateTime.Now - lastCoreRequest)).Wait();
            }
        }

        private void WaitForNextSearchRequest(int delayMultiply = 1)
        {
            if (DateTime.Now - lastSearchRequest < searchRequestDelay * delayMultiply)
            {
                Task.Delay((searchRequestDelay * delayMultiply) - (DateTime.Now - lastSearchRequest)).Wait();
            }
        }

        private void HandleAggregateExceptionForCore(int abuseDelay, AggregateException aggregateException)
        {
            foreach (var exception in aggregateException.InnerExceptions)
            {
                if (exception is AbuseException)
                {
                    Console.WriteLine($"{nameof(AbuseException)} detected, Wait for {abuseDelay} minute");
                    Task.Delay(TimeSpan.FromMinutes(abuseDelay)).Wait();
                    break;
                }
                else if (exception is RateLimitExceededException)
                {
                    Console.WriteLine($"{nameof(RateLimitExceededException)} detected, Wait for next reset");
                    Task.Delay((CoreRateLimit.Reset.LocalDateTime - DateTime.Now).Add(TimeSpan.FromSeconds(3))).Wait();
                    break;
                }
            }
        }

        private void HandleAggregateExceptionForSearch(int abuseDelay, AggregateException aggregateException)
        {
            foreach (var exception in aggregateException.InnerExceptions)
            {
                if (exception is AbuseException)
                {
                    Console.WriteLine($"{nameof(AbuseException)} detected, Wait for {abuseDelay} minute");
                    Task.Delay(TimeSpan.FromMinutes(abuseDelay)).Wait();
                    break;
                }
                else if (exception is RateLimitExceededException)
                {
                    Console.WriteLine($"{nameof(RateLimitExceededException)} detected, Wait for next reset");
                    Task.Delay((SearchRateLimit.Reset.LocalDateTime - DateTime.Now).Add(TimeSpan.FromSeconds(3))).Wait();
                    break;
                }
            }
        }
    }
}
