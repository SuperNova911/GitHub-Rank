using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GitHubRankWeb.Core;
using GitHubRankWeb.Models;
using Microsoft.AspNetCore.Mvc;

namespace GitHubRankWeb.Controllers
{
    public class RankingController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Star(string type, int page)
        {
            page = Math.Clamp(page, 0, 10);
            ViewData["type"] = type;
            ViewData["page"] = page;

            List<RankItem> mostStarItems;
            switch (type)
            {
                case "user":
                    mostStarItems = DataCollection.Instance.MostStarUsers;
                    break;
                case "organization":
                    mostStarItems = DataCollection.Instance.MostStarOrganizations;
                    break;
                case "repository":
                    mostStarItems = DataCollection.Instance.MostStarRepositories;
                    break;
                default:
                    ViewData["type"] = "user";
                    mostStarItems = DataCollection.Instance.MostStarUsers;
                    break;
            }

            return View(mostStarItems);
        }

        public IActionResult Fork(string type)
        {
            ViewData["type"] = type;

            List<RankItem> mostForkItems;
            switch (type)
            {
                case "user":
                    mostForkItems = DataCollection.Instance.MostForkUsers;
                    break;
                case "organization":
                    mostForkItems = DataCollection.Instance.MostForkOrganizations;
                    break;
                case "repository":
                    mostForkItems = DataCollection.Instance.MostForkRepositories;
                    break;
                default:
                    ViewData["type"] = "user";
                    mostForkItems = DataCollection.Instance.MostForkUsers;
                    break;
            }

            return View(mostForkItems);
        }

        public IActionResult Repo_Count(string type)
        {
            ViewData["type"] = type;

            List<RankItem> mostRepoItems;
            switch (type)
            {
                case "user":
                    mostRepoItems = DataCollection.Instance.MostRepoUsers;
                    break;
                case "organization":
                    mostRepoItems = DataCollection.Instance.MostRepoOrganizations;
                    break;
                default:
                    ViewData["type"] = "user";
                    mostRepoItems = DataCollection.Instance.MostRepoUsers;
                    break;
            }

            return View(mostRepoItems);
        }

        public IActionResult Follower()
        {
            List<RankItem> mostFollowerItems = DataCollection.Instance.MostFollowerUsers;
            return View(mostFollowerItems);
        }

        public IActionResult Following()
        {
            List<RankItem> mostFollowingItems = DataCollection.Instance.MostFollowingUsers;
            return View(mostFollowingItems);
        }
    }
}