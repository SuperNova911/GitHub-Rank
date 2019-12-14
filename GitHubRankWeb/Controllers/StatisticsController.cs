using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GitHubRankWeb.Core;
using GitHubRankWeb.Models;
using Microsoft.AspNetCore.Mvc;

namespace GitHubRankWeb.Controllers
{
    public class StatisticsController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Language()
        {
            List<LanguageModel> mostLanguages = DataCollection.Instance.MostLanguages;
            return View(mostLanguages);
        }

        public IActionResult License()
        {
            List<LicenseModel> mostLicenses = DataCollection.Instance.MostLicenses;
            return View(mostLicenses);
        }
    }
}