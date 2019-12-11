using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
            return View();
        }

        public IActionResult Commit()
        {
            return View();
        }

        public IActionResult License()
        {
            return View();
        }
    }
}