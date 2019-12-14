using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GitHubDataCollector;
using GitHubRankWeb.Core;
using GitHubRankWeb.Models;
using Microsoft.AspNetCore.Mvc;

namespace GitHubRankWeb.Controllers
{
    public class AccountController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }

        public new IActionResult User(string login)
        {
            ViewData["login"] = login;

            User user = DatabaseManagerWeb.Instance.User_SelectByLogin(login);
            if (user != null)
            {
                List<RepositoryModel> repos = DatabaseManagerWeb.Instance.Repositories_SelectAllByOwnerLogin(login, 100)
                    .Select(e => new RepositoryModel
                    {
                        Name = e.Name,
                        OwnerName = login,
                        Description = e.Description,
                        Language = e.Language,
                        Star = e.StargazersCount,
                        Fork = e.ForksCount,
                        HtmlUrl = e.HtmlUrl,
                        Reference = $"/account/user?login={login}"
                    }).ToList();

                int totalStar = DatabaseManagerWeb.Instance.Account_TotalStar(user.Login);
                int totalFork = DatabaseManagerWeb.Instance.Account_TotalFork(user.Login);

                var userModel = new UserModel
                {
                    AvatarUrl = user.AvatarUrl,
                    Name = user.Login,
                    Star = totalStar,
                    Fork = totalFork,
                    Follower = user.Followers,
                    Following = user.Following,
                    Email = user.Email,
                    Bio = user.Bio,
                    Company = user.Company,
                    Location = user.Location,
                    BlogUrl = user.BlogUrl,
                    HtmlUrl = user.HtmlUrl,
                    Reference = $"/account/user?login={user.Login}",
                    CreatedAt = user.CreatedAt,
                    FetchedAt = user.FetchedAt,
                    RepositoryModels = repos,
                };

                return View(userModel);
            }
            else
            {
                Organization org = DatabaseManagerWeb.Instance.Organization_SelectByLogin(login);
                if (org != null)
                {
                    return Redirect($"/account/organization?login={login}");
                }
                else
                {
                    UserModel userModel = FetchUser(login);
                    if (userModel != null)
                    {
                        return View(userModel);
                    }
                    else
                    {
                        OrganizationModel orgModel = FetchOrganization(login);
                        if (orgModel != null)
                        {
                            return Redirect($"/account/organization?login={login}");
                        }
                        else
                        {
                            return View();
                        }
                    }
                }
            }
        }

        public IActionResult Organization(string login)
        {
            ViewData["login"] = login;

            Organization org = DatabaseManagerWeb.Instance.Organization_SelectByLogin(login);
            if (org != null)
            {
                List<RepositoryModel> repoModels = DatabaseManagerWeb.Instance.Repositories_SelectAllByOwnerLogin(login, 100)
                    .Select(e => new RepositoryModel
                    {
                        Name = e.Name,
                        OwnerName = login,
                        Description = e.Description,
                        Language = e.Language,
                        Star = e.StargazersCount,
                        Fork = e.ForksCount,
                        HtmlUrl = e.HtmlUrl,
                        Reference = $"/account/organization?login={login}"
                    }).ToList();

                int totalStar = DatabaseManagerWeb.Instance.Account_TotalStar(org.Login);
                int totalFork = DatabaseManagerWeb.Instance.Account_TotalFork(org.Login);

                var orgModel = new OrganizationModel
                {
                    AvatarUrl = org.AvatarUrl,
                    Name = org.Login,
                    Star = totalStar,
                    Fork = totalFork,
                    Email = org.Email,
                    Location = org.Location,
                    HtmlUrl = org.HtmlUrl,
                    BlogUrl = org.BlogUrl,
                    Reference = $"/account/organization?login={org.Login}",
                    CreatedAt = org.CreatedAt,
                    FetchedAt = org.FetchedAt,
                    RepositoryModels = repoModels,
                };

                return View(orgModel);
            }
            else
            {
                User user = DatabaseManagerWeb.Instance.User_SelectByLogin(login);
                if (user != null)
                {
                    return Redirect($"/account/user?login={login}");
                }
                else
                {
                    OrganizationModel orgModel = FetchOrganization(login);
                    if (orgModel != null)
                    {
                        return View(orgModel);
                    }
                    else
                    {
                        UserModel userModel = FetchUser(login);
                        if (userModel != null)
                        {
                            return Redirect($"/account/user?login={login}");
                        }
                        else
                        {
                            return View();
                        }
                    }
                }
            }
        }

        public UserModel FetchUser(string login)
        {
            Octokit.User o_user = GitHubAPI.Instance.User_GetByLogin(login);
            if (o_user == null || o_user.Type != Octokit.AccountType.User)
            {
                return null;
            }

            User user = new User(o_user, true);
            DatabaseManagerWeb.Instance.Account_InsertOrUpdate(user);

            List<RepositoryModel> repoModels = null;
            IReadOnlyList<Octokit.Repository> o_repos = GitHubAPI.Instance.Repositories_GetAllForUser(login, 1);
            if (o_repos != null)
            {
                List<Repository> repos = o_repos.Select(e => new Repository(e, true)).ToList();
                DatabaseManagerWeb.Instance.Repository_InsertOrUpdate(repos);

                repoModels = repos.Select(repo => new RepositoryModel
                {
                    Name = repo.Name,
                    OwnerName = login,
                    Description = repo.Description,
                    Language = repo.Language,
                    Star = repo.StargazersCount,
                    Fork = repo.ForksCount,
                    HtmlUrl = repo.HtmlUrl,
                    Reference = $"/account/user?login={login}"
                }).ToList();
            }

            int totalStar = o_repos.Select(e => e.StargazersCount).Sum();
            int totalFork = o_repos.Select(e => e.ForksCount).Sum();

            var userModel = new UserModel
            {
                AvatarUrl = user.AvatarUrl,
                Name = user.Login,
                Star = totalStar,
                Fork = totalFork,
                Follower = user.Followers,
                Following = user.Following,
                Email = user.Email,
                Bio = user.Bio,
                Company = user.Company,
                Location = user.Location,
                BlogUrl = user.BlogUrl,
                HtmlUrl = user.HtmlUrl,
                Reference = $"/account/user?login={user.Login}",
                CreatedAt = user.CreatedAt,
                FetchedAt = user.FetchedAt,
                RepositoryModels = repoModels ?? new List<RepositoryModel>(),
            };

            return userModel;
        }

        public OrganizationModel FetchOrganization(string login)
        {
            Octokit.Organization o_org = GitHubAPI.Instance.Organization_GetByLogin(login);
            if (o_org == null || o_org.Type != Octokit.AccountType.Organization)
            {
                return null;
            }

            Organization org = new Organization(o_org, true);
            DatabaseManagerWeb.Instance.Account_InsertOrUpdate(org);

            List<RepositoryModel> repoModels = null;
            IReadOnlyList<Octokit.Repository> o_repos = GitHubAPI.Instance.Repositories_GetAllForOrg(login, 1);
            if (o_repos != null)
            {
                List<Repository> repos = o_repos.Select(o_repo => new Repository(o_repo, true)).ToList();
                DatabaseManagerWeb.Instance.Repository_InsertOrUpdate(repos);

                repoModels = repos.Select(repo => new RepositoryModel
                {
                    Name = repo.Name,
                    OwnerName = login,
                    Description = repo.Description,
                    Language = repo.Language,
                    Star = repo.StargazersCount,
                    Fork = repo.ForksCount,
                    HtmlUrl = repo.HtmlUrl,
                    Reference = $"/account/organization?login={login}"
                }).ToList();
            }

            int totalStar = o_repos.Select(e => e.StargazersCount).Sum();
            int totalFork = o_repos.Select(e => e.ForksCount).Sum();

            var orgModel = new OrganizationModel
            {
                AvatarUrl = org.AvatarUrl,
                Name = org.Login,
                Star = totalStar,
                Fork = totalFork,
                Email = org.Email,
                Location = org.Location,
                BlogUrl = org.BlogUrl,
                HtmlUrl = org.HtmlUrl,
                Reference = $"/account/organization?login={org.Login}",
                CreatedAt = org.CreatedAt,
                FetchedAt = org.FetchedAt,
                RepositoryModels = repoModels ?? new List<RepositoryModel>(),
            };

            return orgModel;
        }
    }
}